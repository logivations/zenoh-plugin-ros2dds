// Copyright (c) 2026 Logivations
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

//! Matching is an observation, not permission for a callback to mutate DDS.
//! The route manager owns resources and calls `reconcile`. Each route allocates
//! its own observation cell: an old listener can never address a replacement.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use serde::{Serialize, Serializer};
use tokio::sync::Notify;

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

/// One earliest deadline for the existing route owner, not one task per route.
/// Completed requests and retired routes may leave a harmless stale wake.
#[derive(Default)]
pub(crate) struct Maintenance {
    deadline: Mutex<Option<Instant>>,
    changed: Notify,
}

impl Maintenance {
    pub(crate) fn arm(&self, deadline: Instant) {
        let earlier = {
            let mut next = self.deadline.lock().unwrap_or_else(|e| e.into_inner());
            if next.is_none_or(|current| deadline < current) {
                *next = Some(deadline);
                true
            } else {
                false
            }
        };
        if earlier {
            self.changed.notify_one();
        }
    }

    /// A selected reconciliation clears the hint before scanning. Each pending
    /// retry/deadline re-arms it; concurrent callbacks can safely arm it too.
    pub(crate) fn begin_reconcile(&self) {
        self.deadline
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
    }

    pub(crate) async fn wait(&self, not_before: Instant) {
        loop {
            let deadline = *self.deadline.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(deadline) = deadline {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline.max(not_before).into()) => return,
                    _ = self.changed.notified() => {},
                }
            } else {
                self.changed.notified().await;
            }
            // Notify only changes the sleep. Do not scan for every request,
            // or consume a due hint in a future the owner's select may cancel.
        }
    }
}

#[derive(Serialize)]
pub(crate) struct RouteLifecycle {
    generation: u64,
    #[serde(rename = "desired", serialize_with = "serialize_matching")]
    matching: Arc<AtomicBool>,
    #[serde(flatten)]
    retry: Retry,
}

/// Backoff is state on the owner, never another task or callback.
#[derive(Serialize)]
pub(crate) struct Retry {
    activation_failures: u64,
    consecutive_failures: u32,
    last_error: Option<String>,
    #[serde(skip)]
    retry_at: Instant,
    #[serde(skip)]
    maintenance: Arc<Maintenance>,
}

impl Retry {
    pub(crate) fn new(maintenance: Arc<Maintenance>) -> Self {
        Self {
            activation_failures: 0,
            consecutive_failures: 0,
            last_error: None,
            retry_at: Instant::now(),
            maintenance,
        }
    }
    pub(crate) fn ready_or_schedule(&self) -> bool {
        self.ready_or_schedule_at(Instant::now())
    }
    fn ready_or_schedule_at(&self, now: Instant) -> bool {
        if now >= self.retry_at {
            true
        } else {
            self.maintenance.arm(self.retry_at);
            false
        }
    }
    pub(crate) fn record(&mut self, result: Result<(), String>) -> Result<(), String> {
        self.record_at(result, Instant::now())
    }
    fn record_at(&mut self, result: Result<(), String>, now: Instant) -> Result<(), String> {
        match result {
            Ok(()) => {
                self.consecutive_failures = 0;
                self.last_error = None;
                self.retry_at = now;
                Ok(())
            }
            Err(error) => {
                self.activation_failures = self.activation_failures.saturating_add(1);
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                let shift = (self.consecutive_failures - 1).min(6);
                self.retry_at = now + Duration::from_millis((100u64 << shift).min(5000));
                self.maintenance.arm(self.retry_at);
                self.last_error = Some(error.clone());
                Err(error)
            }
        }
    }
}

fn serialize_matching<S: Serializer>(value: &Arc<AtomicBool>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_bool(value.load(Ordering::Acquire))
}

impl RouteLifecycle {
    pub(crate) fn new(maintenance: Arc<Maintenance>) -> Self {
        Self {
            generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            matching: Arc::new(AtomicBool::new(false)),
            retry: Retry::new(maintenance),
        }
    }

    pub(crate) fn observer(&self, wake: Arc<Notify>) -> impl Fn(bool) + Send + Sync + 'static {
        let state = Arc::downgrade(&self.matching);
        move |status| {
            if let Some(state) = state.upgrade() {
                // Never block on a lifecycle/session lock, including during
                // listener declaration or undeclaration (#382/#533).
                state.store(status, Ordering::Release);
                wake.notify_one();
            }
        }
    }

    pub(crate) fn desired(&self) -> bool {
        self.matching.load(Ordering::Acquire)
    }

    pub(crate) fn set_desired(&mut self, desired: bool) {
        self.matching.store(desired, Ordering::Release);
    }

    pub(crate) fn log_activation_failure(&self, route_id: &str, error: &str) {
        tracing::error!(
            generation = self.generation,
            consecutive_failures = self.retry.consecutive_failures,
            activation_failures = self.retry.activation_failures,
            "{route_id}: activation failed: {error}"
        );
    }

    pub(crate) fn reconcile<T>(
        &mut self,
        actual: &mut Option<T>,
        create: impl FnOnce() -> Result<T, String>,
    ) -> Result<(), String> {
        // Stable routes need neither a clock read nor another retry-state write.
        // A withdrawn failed activation must still clear its previous backoff.
        if self.retry.consecutive_failures == 0 && self.desired() == actual.is_some() {
            return Ok(());
        }
        self.reconcile_at(actual, Instant::now(), create)
    }

    fn reconcile_at<T>(
        &mut self,
        actual: &mut Option<T>,
        now: Instant,
        create: impl FnOnce() -> Result<T, String>,
    ) -> Result<(), String> {
        if !self.desired() {
            actual.take();
            self.retry.record_at(Ok(()), now)?;
        } else if actual.is_none() && self.retry.ready_or_schedule_at(now) {
            match create() {
                Ok(resources) => {
                    // Matching may change during a DDS call. Publish only if
                    // still desired; otherwise the uncommitted bundle drops.
                    if self.desired() {
                        *actual = Some(resources);
                    }
                    self.retry.record_at(Ok(()), now)?;
                }
                Err(error) => {
                    return self.retry.record_at(Err(error), now);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use futures::{poll, FutureExt};

    use super::*;

    async fn due(maintenance: &Maintenance) {
        tokio::time::timeout(Duration::from_secs(1), maintenance.wait(Instant::now()))
            .await
            .expect("Maintenance deadline was lost");
    }

    #[test]
    fn maintenance_wake_survives_cancellation_and_consumes_no_deadline() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let maintenance = Maintenance::default();
            let mut waiting = Box::pin(maintenance.wait(Instant::now()));
            assert!(poll!(waiting.as_mut()).is_pending());
            maintenance.arm(Instant::now());
            // Another owner event wins select after the notification. Dropping
            // its wait future must not discard the pending deadline.
            drop(waiting);
            due(&maintenance).await;
            due(&maintenance).await;
            maintenance.begin_reconcile();
            assert!(maintenance.wait(Instant::now()).now_or_never().is_none());
        });
    }

    #[test]
    fn earlier_deadline_reschedules_sleep_and_arms_during_scan_survive() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let maintenance = Maintenance::default();
            let later = Instant::now() + Duration::from_secs(3600);
            maintenance.arm(later);
            let mut waiting = Box::pin(maintenance.wait(Instant::now()));
            assert!(poll!(waiting.as_mut()).is_pending());
            maintenance.arm(Instant::now());
            maintenance.arm(later);
            tokio::time::timeout(Duration::from_secs(1), waiting)
                .await
                .unwrap();
            maintenance.begin_reconcile();
            // A callback may arm another deadline while the owner is scanning.
            maintenance.arm(Instant::now());
            due(&maintenance).await;
            maintenance.begin_reconcile();
            assert!(maintenance.wait(Instant::now()).now_or_never().is_none());
        });
    }

    #[test]
    fn earlier_or_overdue_hints_cannot_bypass_the_scan_floor() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let maintenance = Maintenance::default();
            let now = Instant::now();
            let floor = now + Duration::from_secs(3600);
            maintenance.arm(floor + Duration::from_secs(3600));
            let mut waiting = Box::pin(maintenance.wait(floor));
            assert!(poll!(waiting.as_mut()).is_pending());
            maintenance.arm(now);
            assert!(poll!(waiting.as_mut()).is_pending());
            drop(waiting);
            // The hint was preserved; an elapsed floor allows it to run.
            due(&maintenance).await;
        });
    }

    struct Resource(Arc<AtomicUsize>);
    impl Drop for Resource {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    fn create(live: &Arc<AtomicUsize>) -> Result<Resource, String> {
        live.fetch_add(1, Ordering::SeqCst);
        Ok(Resource(live.clone()))
    }

    #[test]
    fn duplicate_true_and_remote_churn_do_not_replace_resources() {
        let mut owner = RouteLifecycle::new(Arc::new(Maintenance::default()));
        let live = Arc::new(AtomicUsize::new(0));
        let mut actual = None;
        owner.matching.store(true, Ordering::Release);
        owner.reconcile(&mut actual, || create(&live)).unwrap();
        for _ in 0..100 {
            owner.matching.store(true, Ordering::Release);
            owner
                .reconcile(&mut actual, || panic!("duplicate activation"))
                .unwrap();
        }
        assert_eq!(live.load(Ordering::SeqCst), 1);
        owner.matching.store(false, Ordering::Release);
        owner
            .reconcile(&mut actual, || panic!("inactive creation"))
            .unwrap();
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn matching_change_during_creation_rolls_back_unpublished_bundle() {
        let mut owner = RouteLifecycle::new(Arc::new(Maintenance::default()));
        let live = Arc::new(AtomicUsize::new(0));
        let observation = owner.matching.clone();
        observation.store(true, Ordering::Release);
        let mut actual = None;
        owner
            .reconcile(&mut actual, || {
                let resource = create(&live)?;
                observation.store(false, Ordering::Release);
                Ok(resource)
            })
            .unwrap();
        assert!(actual.is_none());
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn old_observation_cannot_mutate_replacement_generation() {
        let maintenance = Arc::new(Maintenance::default());
        let old = RouteLifecycle::new(maintenance.clone());
        let stale = old.observer(Arc::new(Notify::new()));
        let generation = old.generation;
        drop(old);
        let replacement = RouteLifecycle::new(maintenance);
        stale(true);
        assert_ne!(generation, replacement.generation);
        assert!(!replacement.desired());
    }

    #[test]
    fn activation_retries_without_another_matching_edge() {
        let maintenance = Arc::new(Maintenance::default());
        let mut owner = RouteLifecycle::new(maintenance.clone());
        let now = Instant::now();
        let live = Arc::new(AtomicUsize::new(0));
        let mut actual = None;
        owner.matching.store(true, Ordering::Release);
        assert!(owner
            .reconcile_at::<Resource>(&mut actual, now, || Err("reader failure".into()))
            .is_err());
        assert_eq!(
            *maintenance.deadline.lock().unwrap(),
            Some(now + Duration::from_millis(100))
        );
        // A matching-triggered scan before the retry is due must re-arm it.
        maintenance.begin_reconcile();
        owner
            .reconcile_at(&mut actual, now, || panic!("retry without backoff"))
            .unwrap();
        assert_eq!(
            *maintenance.deadline.lock().unwrap(),
            Some(now + Duration::from_millis(100))
        );
        maintenance.begin_reconcile();
        owner
            .reconcile_at(&mut actual, now + Duration::from_millis(100), || {
                create(&live)
            })
            .unwrap();
        assert!(actual.is_some());
        assert!(owner.retry.last_error.is_none());
        assert_eq!(owner.retry.activation_failures, 1);
        assert!(maintenance.deadline.lock().unwrap().is_none());
    }

    #[test]
    fn withdrawn_failed_activation_clears_backoff_before_matching_returns() {
        let mut owner = RouteLifecycle::new(Arc::new(Maintenance::default()));
        let mut actual = None;
        owner.set_desired(true);
        owner
            .reconcile::<()>(&mut actual, || Err("creation failed".into()))
            .unwrap_err();
        assert_eq!(owner.retry.consecutive_failures, 1);

        owner.set_desired(false);
        owner
            .reconcile(&mut actual, || panic!("creation without demand"))
            .unwrap();
        assert_eq!(owner.retry.consecutive_failures, 0);
        assert!(owner.retry.last_error.is_none());

        owner.set_desired(true);
        owner.reconcile(&mut actual, || Ok(())).unwrap();
        assert!(
            actual.is_some(),
            "withdrawn failure must not delay new demand"
        );
    }
}
