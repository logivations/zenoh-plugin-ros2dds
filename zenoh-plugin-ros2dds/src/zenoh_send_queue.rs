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

use tokio::task::JoinHandle;

use crate::spawn_runtime;

// Sends a route keeps while one of its sends is blocked. Further sends are dropped:
// the ROS client, or the remote querier, then runs into its usual timeout.
const MAX_PENDING_SENDS: usize = 16;

type ZenohSend = Box<dyn FnOnce() + Send + 'static>;

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
}

impl ZenohSendQueue {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = flume::bounded::<ZenohSend>(MAX_PENDING_SENDS);
        let worker = spawn_runtime(async move {
            while let Ok(send) = receiver.recv_async().await {
                // a blocked send occupies a thread of the blocking pool, not a worker
                if let Err(e) = tokio::task::spawn_blocking(send).await {
                    tracing::error!("Zenoh send of a Service route failed: {e}");
                }
            }
        });
        ZenohSendQueue { sender, worker }
    }

    pub(crate) fn sender(&self) -> ZenohSender {
        ZenohSender(self.sender.clone())
    }
}

impl Drop for ZenohSendQueue {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

/// Queues sends for a [`ZenohSendQueue`] without ever blocking the caller.
#[derive(Clone)]
pub(crate) struct ZenohSender(flume::Sender<ZenohSend>);

impl ZenohSender {
    pub(crate) fn send(&self, route_id: &str, send: impl FnOnce() + Send + 'static) {
        match self.0.try_send(Box::new(send)) {
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
}
