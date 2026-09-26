// Copyright (c) 2026 Logivations
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

//! Matching is an observation, not permission for a callback to mutate DDS.
//! The route manager owns resources and calls `reconcile`. Each route allocates
//! its own observation cell: an old listener can never address a replacement.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use serde::{Serialize, Serializer};
use tokio::sync::Notify;

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Serialize)]
pub(crate) struct RouteLifecycle {
    generation: u64,
    #[serde(serialize_with = "serialize_matching")]
    matching: Arc<AtomicBool>,
    activation_failures: u64,
    consecutive_failures: u32,
    last_error: Option<String>,
    #[serde(skip)]
    retry_at: Instant,
}

fn serialize_matching<S: Serializer>(value: &Arc<AtomicBool>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_bool(value.load(Ordering::Acquire))
}

impl RouteLifecycle {
    pub(crate) fn new() -> Self {
        Self {
            generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            matching: Arc::new(AtomicBool::new(false)),
            activation_failures: 0,
            consecutive_failures: 0,
            last_error: None,
            retry_at: Instant::now(),
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

    pub(crate) fn reconcile<T>(
        &mut self,
        actual: &mut Option<T>,
        create: impl FnOnce() -> Result<T, String>,
    ) -> Result<(), String> {
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
            self.consecutive_failures = 0;
            self.last_error = None;
            self.retry_at = now;
        } else if actual.is_none() && now >= self.retry_at {
            match create() {
                Ok(resources) => {
                    // Matching may change during a DDS call. Publish only if
                    // still desired; otherwise the uncommitted bundle drops.
                    if self.desired() {
                        *actual = Some(resources);
                    }
                    self.consecutive_failures = 0;
                    self.last_error = None;
                    self.retry_at = now;
                }
                Err(error) => {
                    self.activation_failures = self.activation_failures.saturating_add(1);
                    self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                    let shift = (self.consecutive_failures - 1).min(6);
                    self.retry_at = now + Duration::from_millis((100u64 << shift).min(5000));
                    self.last_error = Some(error.clone());
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

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
        let mut owner = RouteLifecycle::new();
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
        let mut owner = RouteLifecycle::new();
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
        let old = RouteLifecycle::new();
        let stale = old.observer(Arc::new(Notify::new()));
        let generation = old.generation;
        drop(old);
        let replacement = RouteLifecycle::new();
        stale(true);
        assert_ne!(generation, replacement.generation);
        assert!(!replacement.desired());
    }

    #[test]
    fn activation_retries_without_another_matching_edge() {
        let mut owner = RouteLifecycle::new();
        let now = Instant::now();
        let live = Arc::new(AtomicUsize::new(0));
        let mut actual = None;
        owner.matching.store(true, Ordering::Release);
        assert!(owner
            .reconcile_at::<Resource>(&mut actual, now, || Err("reader failure".into()))
            .is_err());
        owner
            .reconcile_at(&mut actual, now, || panic!("retry without backoff"))
            .unwrap();
        owner
            .reconcile_at(&mut actual, now + Duration::from_millis(100), || {
                create(&live)
            })
            .unwrap();
        assert!(actual.is_some());
        assert!(owner.last_error.is_none());
        assert_eq!(owner.activation_failures, 1);
    }
}
