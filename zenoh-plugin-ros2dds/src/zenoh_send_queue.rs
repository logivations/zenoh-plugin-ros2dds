//
// Copyright (c) 2026 Logivations GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use tokio::task::JoinHandle;

use crate::spawn_runtime;

// Sends a route keeps while one of its sends is blocked. Further sends are dropped:
// the ROS client, or the remote querier, then runs into its usual timeout.
const MAX_PENDING_SENDS: usize = 16;

// Drop the payload before returning its reservation, including on queue rejection.
type ZenohSend = (Box<dyn FnOnce() + Send + 'static>, SendReservation);

/// One budget per bridge, shared by every service request/reply queue.
pub(crate) struct SendBudget {
    limit: usize,
    used: AtomicUsize,
}

impl SendBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<SendReservation> {
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                // Permit one oversized payload only when otherwise empty, so
                // a valid large response is not permanently impossible to send.
                used.checked_add(bytes)
                    .filter(|&total| used == 0 || total <= self.limit)
            })
            .ok()
            .map(|_| SendReservation(self.clone(), bytes))
    }
}

struct SendReservation(Arc<SendBudget>, usize);

impl Drop for SendReservation {
    fn drop(&mut self) {
        self.0.used.fetch_sub(self.1, Ordering::Relaxed);
    }
}

/// Runs the blocking Zenoh sends of a Service route (`get().wait()`, `reply().wait()`)
/// outside of the DDS listener.
///
/// CycloneDDS calls the listeners of all the bridge's DDS Readers from one thread
/// (`dq.user`). Queries and replies use `CongestionControl::Block`: while the TX queue
/// towards their destination is full they wait up to
/// `transport/link/tx/queue/congestion_control/block/wait_before_close`, and every DDS
/// sample of the bridge (all topics, all services) waited with them. Each route runs
/// its sends in order on its own task instead, so a congested destination only delays
/// the routes that send to it.
pub(crate) struct ZenohSendQueue {
    sender: flume::Sender<ZenohSend>,
    worker: JoinHandle<()>,
    generation: Arc<Mutex<u64>>,
    budget: Arc<SendBudget>,
}

impl ZenohSendQueue {
    pub(crate) fn new(budget: Arc<SendBudget>) -> Self {
        let (sender, receiver) = flume::bounded::<ZenohSend>(MAX_PENDING_SENDS);
        let worker = spawn_runtime(async move {
            while let Ok(send) = receiver.recv_async().await {
                // a blocked send occupies a thread of the blocking pool, not a worker
                if let Err(e) = tokio::task::spawn_blocking(move || {
                    let (send, _reservation) = send;
                    send();
                })
                .await
                {
                    tracing::error!("Zenoh send of a Service route failed: {e}");
                }
            }
        });
        ZenohSendQueue {
            sender,
            worker,
            generation: Arc::new(Mutex::new(0)),
            budget,
        }
    }

    pub(crate) fn sender(&self) -> ZenohSender {
        ZenohSender {
            sender: self.sender.clone(),
            budget: self.budget.clone(),
            generation: SendGeneration {
                current: self.generation.clone(),
                expected: *self.generation.lock().unwrap_or_else(|e| e.into_inner()),
            },
        }
    }

    /// Revoke queued work and reply callbacks before deleting DDS resources.
    /// Does not wait for a blocking Zenoh send that has already started.
    pub(crate) fn invalidate(&self) {
        *self.generation.lock().unwrap_or_else(|e| e.into_inner()) += 1;
    }
}

impl Drop for ZenohSendQueue {
    fn drop(&mut self) {
        self.invalidate();
        self.worker.abort();
    }
}

/// Queues sends for a [`ZenohSendQueue`] without ever blocking the caller.
#[derive(Clone)]
pub(crate) struct ZenohSender {
    sender: flume::Sender<ZenohSend>,
    budget: Arc<SendBudget>,
    pub(crate) generation: SendGeneration,
}

// A callback retains only the generation, never a Sender that would keep
// its own queue alive through a queued closure.
#[derive(Clone)]
pub(crate) struct SendGeneration {
    current: Arc<Mutex<u64>>,
    expected: u64,
}

impl SendGeneration {
    /// Serialize DDS reply access with invalidation. Never hold this guard
    /// across a blocking Zenoh send or DDS entity deletion.
    pub(crate) fn if_current<R>(&self, op: impl FnOnce() -> R) -> Option<R> {
        let generation = self.current.lock().unwrap_or_else(|e| e.into_inner());
        if *generation == self.expected {
            Some(op())
        } else {
            None
        }
    }
}

impl ZenohSender {
    pub(crate) fn send(&self, route_id: &str, bytes: usize, send: impl FnOnce() + Send + 'static) {
        let Some(reservation) = self.budget.reserve(bytes) else {
            tracing::warn!(
                "{route_id}: service send byte budget ({}) exhausted - dropping {bytes}-byte send to Zenoh",
                self.budget.limit
            );
            return;
        };
        let generation = self.generation.clone();
        let send = Box::new(move || {
            // Claim on the blocking worker, not when queued: aborting its
            // async parent cannot cancel a queued spawn_blocking closure.
            // Once claimed it is in flight; its DDS reply needs its own guard.
            if generation.if_current(|| ()).is_some() {
                send();
            }
        });
        match self.sender.try_send((send, reservation)) {
            Ok(()) => {}
            Err(flume::TrySendError::Full(_)) => tracing::warn!(
                "{route_id}: {MAX_PENDING_SENDS} sends to Zenoh are still pending (congested destination?) - dropping this one"
            ),
            Err(flume::TrySendError::Disconnected(_)) => {
                tracing::debug!("{route_id}: route is gone - dropping send to Zenoh")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{mpsc, Arc, Mutex},
        time::{Duration, Instant},
    };

    use super::*;

    const WAIT: Duration = Duration::from_secs(5);

    fn queue() -> ZenohSendQueue {
        ZenohSendQueue::new(Arc::new(SendBudget::new(64 * 1024 * 1024)))
    }

    fn assert_released(budget: &SendBudget) {
        let deadline = Instant::now() + WAIT;
        while budget.used.load(Ordering::Relaxed) != 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(budget.used.load(Ordering::Relaxed), 0);
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    // A send blocked on a congested destination (here: until `release` is sent)
    // must neither block the DDS listener that queued it nor the sends of other
    // routes, and the route's later sends must still go out in order.
    #[test]
    fn blocked_send_delays_only_its_own_route() {
        let rt = runtime();
        let _guard = rt.enter();
        let congested = queue();
        let other = queue();
        let sent = Arc::new(Mutex::new(Vec::new()));
        let (release, released) = mpsc::channel::<()>();

        let start = Instant::now();
        {
            let sent = sent.clone();
            congested.sender().send("congested", 1, move || {
                released.recv_timeout(WAIT).unwrap();
                sent.lock().unwrap().push("congested 1");
            });
        }
        for name in ["congested 2", "congested 3"] {
            let sent = sent.clone();
            congested
                .sender()
                .send("congested", 1, move || sent.lock().unwrap().push(name));
        }
        let (other_sent, other_done) = mpsc::channel();
        other
            .sender()
            .send("other", 1, move || other_sent.send(()).unwrap());
        assert!(start.elapsed() < Duration::from_millis(100));

        other_done.recv_timeout(WAIT).unwrap();
        assert!(sent.lock().unwrap().is_empty());

        release.send(()).unwrap();
        let deadline = Instant::now() + WAIT;
        while sent.lock().unwrap().len() < 3 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            *sent.lock().unwrap(),
            ["congested 1", "congested 2", "congested 3"]
        );
    }

    // Once MAX_PENDING_SENDS are waiting behind a blocked send, further sends are
    // dropped instead of blocking the caller.
    #[test]
    fn full_route_drops_sends_without_blocking() {
        let rt = runtime();
        let _guard = rt.enter();
        let queue = queue();
        let sender = queue.sender();
        let (release, released) = mpsc::channel::<()>();
        let (started, has_started) = mpsc::channel();
        sender.send("route", 1, move || {
            started.send(()).unwrap();
            released.recv_timeout(WAIT).unwrap();
        });
        has_started.recv_timeout(WAIT).unwrap();

        let (done, sends_done) = mpsc::channel();
        let start = Instant::now();
        for i in 0..MAX_PENDING_SENDS + 5 {
            let done = done.clone();
            sender.send("route", 1, move || done.send(i).unwrap());
        }
        assert!(start.elapsed() < Duration::from_millis(100));

        assert_eq!(
            queue.budget.used.load(Ordering::Relaxed),
            MAX_PENDING_SENDS + 1
        );

        release.send(()).unwrap();
        drop(done);
        let run: Vec<usize> = sends_done.iter().take(MAX_PENDING_SENDS + 5).collect();
        assert_eq!(run, (0..MAX_PENDING_SENDS).collect::<Vec<_>>());
        assert_released(&queue.budget);
    }

    #[test]
    fn drop_revokes_send_waiting_for_blocking_pool() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let _guard = rt.enter();
        let (release, released) = mpsc::channel();
        let (ready, started) = mpsc::channel();
        let blocker = rt.spawn_blocking(move || {
            ready.send(()).unwrap();
            released.recv_timeout(WAIT).unwrap();
        });
        started.recv_timeout(WAIT).unwrap();
        let queue = queue();
        let (sent, received) = mpsc::channel();
        queue.sender().send("route", 1, move || {
            sent.send(()).unwrap();
        });
        let deadline = Instant::now() + WAIT;
        while !queue.sender.is_empty() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(queue.sender.is_empty(), "worker must dequeue the send");
        let budget = queue.budget.clone();
        let worker = queue.worker.abort_handle();
        drop(queue);
        while !worker.is_finished() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(worker.is_finished());
        // Aborting the async worker cannot free a spawn_blocking job: its
        // payload must remain charged until that job really drops it.
        assert_eq!(budget.used.load(Ordering::Relaxed), 1);
        release.send(()).unwrap();
        rt.block_on(blocker).unwrap();
        assert!(matches!(
            received.recv_timeout(WAIT),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
        assert_released(&budget);
    }

    #[test]
    fn reactivation_discards_old_sends_and_reply_access() {
        let rt = runtime();
        let _guard = rt.enter();
        let queue = queue();
        let old = queue.sender();
        let (release, released) = mpsc::channel();
        let (ready, started) = mpsc::channel();
        old.send("route", 1, move || {
            ready.send(()).unwrap();
            released.recv_timeout(WAIT).unwrap();
        });
        started.recv_timeout(WAIT).unwrap();
        let (sent, received) = mpsc::channel();
        let stale_sent = sent.clone();
        old.send("route", 1, move || {
            stale_sent.send("stale").unwrap();
        });
        queue.invalidate(); // deactivation must not wait for the blocked send
        assert!(old
            .generation
            .if_current(|| panic!("stale reply reached DDS"))
            .is_none());
        let current = queue.sender();
        assert_eq!(current.generation.if_current(|| 42), Some(42));
        current.send("route", 1, move || {
            sent.send("current").unwrap();
        });
        release.send(()).unwrap();
        assert_eq!(received.recv_timeout(WAIT).unwrap(), "current");
        assert!(matches!(
            received.recv_timeout(WAIT),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
        assert_released(&queue.budget);
    }

    #[test]
    fn invalidation_waits_for_in_flight_dds_reply_access() {
        let rt = runtime();
        let _guard = rt.enter();
        let queue = Arc::new(queue());
        let generation = queue.sender().generation;
        let (entered, entering) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let reply = std::thread::spawn(move || {
            generation.if_current(|| {
                entered.send(()).unwrap();
                released.recv_timeout(WAIT).unwrap();
            })
        });
        entering.recv_timeout(WAIT).unwrap();
        let (done, invalidated) = mpsc::channel();
        let retiring = queue.clone();
        let retire = std::thread::spawn(move || {
            retiring.invalidate();
            done.send(()).unwrap();
        });
        assert!(matches!(
            invalidated.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        release.send(()).unwrap();
        assert!(reply.join().unwrap().is_some());
        invalidated.recv_timeout(WAIT).unwrap();
        retire.join().unwrap();
    }

    #[test]
    fn byte_budget_covers_queued_and_in_flight_sends_across_routes() {
        let rt = runtime();
        let _guard = rt.enter();
        let budget = Arc::new(SendBudget::new(8));
        let first = ZenohSendQueue::new(budget.clone());
        let second = ZenohSendQueue::new(budget.clone());
        let (release, released) = mpsc::channel();
        let (started, ready) = mpsc::channel();
        first.sender().send("first", 6, move || {
            started.send(()).unwrap();
            released.recv_timeout(WAIT).unwrap();
        });
        ready.recv_timeout(WAIT).unwrap();
        // The remaining two bytes are held in the same route's queue.
        first.sender().send("first", 2, || {});
        let (sent, received) = mpsc::channel();
        let start = Instant::now();
        second
            .sender()
            .send("second", 1, move || sent.send(()).unwrap());
        assert!(start.elapsed() < Duration::from_millis(100));
        assert!(matches!(
            received.recv_timeout(WAIT),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
        assert_eq!(budget.used.load(Ordering::Relaxed), 8);
        release.send(()).unwrap();
        assert_released(&budget);

        // A panic also releases the reservation and does not stop the worker.
        second
            .sender()
            .send("second", 8, || panic!("injected send failure"));
        assert_released(&budget);
        let (sent, received) = mpsc::channel();
        second
            .sender()
            .send("second", 8, move || sent.send(()).unwrap());
        received.recv_timeout(WAIT).unwrap();
        assert_released(&budget);
    }

    #[test]
    fn concurrent_admission_enforces_budget_and_single_oversized_send() {
        for (bytes, allowed) in [(4, 2), (9, 1), (usize::MAX, 1)] {
            let budget = Arc::new(SendBudget::new(8));
            let barrier = Arc::new(std::sync::Barrier::new(16));
            let jobs: Vec<_> = (0..16)
                .map(|_| {
                    let budget = budget.clone();
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        let reservation = budget.reserve(bytes);
                        barrier.wait(); // everyone attempts admission before anyone releases
                        reservation.is_some()
                    })
                })
                .collect();
            assert_eq!(
                jobs.into_iter()
                    .map(|job| usize::from(job.join().unwrap()))
                    .sum::<usize>(),
                allowed
            );
            assert_released(&budget);
        }
    }

    #[test]
    fn budget_config_defaults_and_rejects_zero() {
        let config: crate::config::Config = serde_json::from_str("{}").unwrap();
        assert_eq!(config.service_send_queue_max_bytes.get(), 64 * 1024 * 1024);
        assert!(serde_json::from_str::<crate::config::Config>(
            r#"{"service_send_queue_max_bytes":0}"#
        )
        .is_err());
    }
}
