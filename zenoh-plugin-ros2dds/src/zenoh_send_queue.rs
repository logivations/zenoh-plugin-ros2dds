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

use std::{
    collections::VecDeque,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, Mutex},
};

use tokio::runtime::Handle;

// Sends a route keeps while one of its sends is blocked. Further sends are dropped:
// the ROS client, or the remote querier, then runs into its usual timeout.
const MAX_PENDING_SENDS: usize = 16;

type ZenohSend = Box<dyn FnOnce() + Send + 'static>;

/// Runs the blocking Zenoh sends of a Service route (`get().wait()`, `reply().wait()`)
/// outside of the DDS listener.
///
/// CycloneDDS can invoke these listeners on its receive thread. Queries and replies
/// use `CongestionControl::Block`: while the TX queue
/// towards their destination is full they wait up to
/// `transport/link/tx/queue/congestion_control/block/wait_before_close`, and every DDS
/// sample of the bridge (all topics, all services) waited with them. Each route runs
/// its sends in order on its own task instead, so a congested destination only delays
/// the routes that send to it.
pub(crate) struct ZenohSendQueue {
    sender: Arc<SendWorker>,
    generation: Arc<Mutex<u64>>,
}

struct SendWorker {
    // None means idle; Some means one blocking worker owns the queue. Enqueue
    // and the worker's transition to idle share this lock so no wakeup is lost.
    pending: Mutex<Option<VecDeque<ZenohSend>>>,
    runtime: Handle,
}

impl ZenohSendQueue {
    pub(crate) fn new() -> Self {
        ZenohSendQueue {
            sender: Arc::new(SendWorker {
                pending: Mutex::new(None),
                runtime: Handle::try_current()
                    .unwrap_or_else(|_| crate::TOKIO_RUNTIME.handle().clone()),
            }),
            generation: Arc::new(Mutex::new(0)),
        }
    }

    pub(crate) fn sender(&self) -> ZenohSender {
        ZenohSender {
            sender: self.sender.clone(),
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
        // The worker disposes revoked sends, too: a captured Zenoh Query may
        // perform a blocking network send in Drop.
    }
}

/// Queues sends for a [`ZenohSendQueue`].
#[derive(Clone)]
pub(crate) struct ZenohSender {
    sender: Arc<SendWorker>,
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
    pub(crate) fn send(&self, route_id: &str, send: impl FnOnce() + Send + 'static) {
        let generation = self.generation.clone();
        let send: ZenohSend = Box::new(move || {
            // Claim on the blocking worker, not when queued. Once claimed it is
            // in flight; its DDS reply needs its own guard.
            if generation.if_current(|| ()).is_some() {
                send();
            }
        });
        let mut pending = self
            .sender
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(queue) = pending.as_mut() {
            if queue.len() < MAX_PENDING_SENDS {
                queue.push_back(send);
            } else {
                // Do not hold the queue lock while dropping captured resources.
                drop(pending);
                tracing::warn!(
                    "{route_id}: {MAX_PENDING_SENDS} sends to Zenoh are still pending (congested destination?) - dropping this one"
                );
            }
            return;
        }
        *pending = Some(VecDeque::new());
        drop(pending);
        let worker = self.sender.clone();
        self.sender.runtime.spawn_blocking(move || worker.run(send));
    }
}

impl SendWorker {
    fn run(&self, mut send: ZenohSend) {
        loop {
            if catch_unwind(AssertUnwindSafe(send)).is_err() {
                tracing::error!("Zenoh send of a Service route panicked");
            }
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            match pending.as_mut().and_then(VecDeque::pop_front) {
                Some(next) => send = next,
                None => {
                    *pending = None;
                    return;
                }
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
        let congested = ZenohSendQueue::new();
        let other = ZenohSendQueue::new();
        let sent = Arc::new(Mutex::new(Vec::new()));
        let (release, released) = mpsc::channel::<()>();

        let start = Instant::now();
        {
            let sent = sent.clone();
            congested.sender().send("congested", move || {
                released.recv_timeout(WAIT).unwrap();
                sent.lock().unwrap().push("congested 1");
            });
        }
        for name in ["congested 2", "congested 3"] {
            let sent = sent.clone();
            congested
                .sender()
                .send("congested", move || sent.lock().unwrap().push(name));
        }
        let (other_sent, other_done) = mpsc::channel();
        other
            .sender()
            .send("other", move || other_sent.send(()).unwrap());
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
        let queue = ZenohSendQueue::new();
        let sender = queue.sender();
        let (release, released) = mpsc::channel::<()>();
        let (started, has_started) = mpsc::channel();
        sender.send("route", move || {
            started.send(()).unwrap();
            released.recv_timeout(WAIT).unwrap();
        });
        has_started.recv_timeout(WAIT).unwrap();

        let (done, sends_done) = mpsc::channel();
        let start = Instant::now();
        for i in 0..MAX_PENDING_SENDS + 5 {
            let done = done.clone();
            sender.send("route", move || done.send(i).unwrap());
        }
        assert!(start.elapsed() < Duration::from_millis(100));

        release.send(()).unwrap();
        drop(done);
        let run: Vec<usize> = sends_done.iter().take(MAX_PENDING_SENDS + 5).collect();
        assert_eq!(run, (0..MAX_PENDING_SENDS).collect::<Vec<_>>());
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
        let queue = ZenohSendQueue::new();
        let (sent, received) = mpsc::channel();
        queue.sender().send("route", move || {
            sent.send(()).unwrap();
        });
        drop(queue);
        release.send(()).unwrap();
        rt.block_on(blocker).unwrap();
        assert!(matches!(
            received.recv_timeout(WAIT),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
    }

    #[test]
    fn reactivation_discards_old_sends_and_reply_access() {
        let rt = runtime();
        let _guard = rt.enter();
        let queue = ZenohSendQueue::new();
        let old = queue.sender();
        let (release, released) = mpsc::channel();
        let (ready, started) = mpsc::channel();
        old.send("route", move || {
            ready.send(()).unwrap();
            released.recv_timeout(WAIT).unwrap();
        });
        started.recv_timeout(WAIT).unwrap();
        let (sent, received) = mpsc::channel();
        let stale_sent = sent.clone();
        old.send("route", move || {
            stale_sent.send("stale").unwrap();
        });
        queue.invalidate(); // deactivation must not wait for the blocked send
        assert!(old
            .generation
            .if_current(|| panic!("stale reply reached DDS"))
            .is_none());
        let current = queue.sender();
        assert_eq!(current.generation.if_current(|| 42), Some(42));
        current.send("route", move || {
            sent.send("current").unwrap();
        });
        release.send(()).unwrap();
        assert_eq!(received.recv_timeout(WAIT).unwrap(), "current");
        assert!(matches!(
            received.recv_timeout(WAIT),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
    }

    #[test]
    fn invalidation_waits_for_in_flight_dds_reply_access() {
        let rt = runtime();
        let _guard = rt.enter();
        let queue = Arc::new(ZenohSendQueue::new());
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
    fn retirement_disposes_queued_resources_on_worker() {
        struct BlockingDrop(mpsc::Sender<std::thread::ThreadId>, mpsc::Receiver<()>);
        impl Drop for BlockingDrop {
            fn drop(&mut self) {
                self.0.send(std::thread::current().id()).unwrap();
                self.1.recv_timeout(WAIT).unwrap();
            }
        }

        let rt = runtime();
        let _guard = rt.enter();
        let queue = ZenohSendQueue::new();
        let (release_send, sending) = mpsc::channel();
        let (started, running) = mpsc::channel();
        queue.sender().send("route", move || {
            started.send(std::thread::current().id()).unwrap();
            sending.recv_timeout(WAIT).unwrap();
        });
        let worker_thread = running.recv_timeout(WAIT).unwrap();
        let (dropping, dropped) = mpsc::channel();
        let (release_drop, disposing) = mpsc::channel();
        let resource = BlockingDrop(dropping, disposing);
        let (executed, execution) = mpsc::channel();
        queue.sender().send("route", move || {
            executed.send(()).unwrap();
            drop(resource);
        });
        drop(queue);
        // Retirement is complete while the first send is still blocked. The
        // queued resource is disposed by that same worker, after it can resume.
        assert!(matches!(dropped.try_recv(), Err(mpsc::TryRecvError::Empty)));
        release_send.send(()).unwrap();
        assert_eq!(dropped.recv_timeout(WAIT).unwrap(), worker_thread);
        release_drop.send(()).unwrap();
        assert!(matches!(
            execution.recv_timeout(WAIT),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
    }

    #[test]
    fn worker_survives_panic_and_idle_enqueue_races() {
        let rt = runtime();
        let _guard = rt.enter();
        let queue = ZenohSendQueue::new();
        let sender = queue.sender();
        let (sent, received) = mpsc::channel();
        sender.send("route", || panic!("injected send panic"));
        // One outstanding send: every enqueue can race with the worker's
        // return to idle, without ever legitimately filling the queue.
        for n in 0..1000 {
            let sent = sent.clone();
            sender.send("route", move || sent.send(n).unwrap());
            assert_eq!(received.recv_timeout(WAIT).unwrap(), n);
        }
    }
}
