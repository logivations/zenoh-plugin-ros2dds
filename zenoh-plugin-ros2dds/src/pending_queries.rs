// Copyright (c) 2026 Logivations
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

//! Expire only deadlines advertised by the caller; legacy/native deadlines are unknown.
//! Retirement releases every pending query, including those with no known deadline.
//! Query destruction can send a Zenoh response-final: always drop outside the lock.
use crate::{ros2_utils::CddsRequestHeader, route_lifecycle::Maintenance};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use zenoh::query::Query;

pub(crate) const RETENTION_PARAMETER: &str = "__ros2dds_timeout_ms";

pub(crate) struct PendingQueries {
    entries: Mutex<HashMap<CddsRequestHeader, (Option<Instant>, Query)>>,
    expired: AtomicU64,
    maintenance: Arc<Maintenance>,
}
impl PendingQueries {
    pub(crate) fn new(maintenance: Arc<Maintenance>) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            expired: AtomicU64::new(0),
            maintenance,
        }
    }
    #[must_use = "Drop the replaced query outside any DDS access guard"]
    pub(crate) fn insert(
        &self,
        id: CddsRequestHeader,
        query: Query,
        deadline: Option<Instant>,
    ) -> Option<Query> {
        let previous = self
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, (deadline, query))
            .map(|(_, query)| query);
        if let Some(deadline) = deadline {
            self.maintenance.arm(deadline);
        }
        previous
    }
    pub(crate) fn take(&self, id: &CddsRequestHeader) -> Option<Query> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id)
            .map(|(_, query)| query)
    }
    pub(crate) fn expire(&self, now: Instant) -> usize {
        let (expired, next) = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            let mut ids = Vec::new();
            let mut next = None;
            for (id, (deadline, _)) in entries.iter() {
                if let Some(deadline) = deadline {
                    if *deadline <= now {
                        ids.push(*id);
                    } else {
                        next =
                            Some(next.map_or(*deadline, |current: Instant| current.min(*deadline)));
                    }
                }
            }
            let expired = ids
                .into_iter()
                .filter_map(|id| entries.remove(&id))
                .collect::<Vec<_>>();
            (expired, next)
        };
        if let Some(deadline) = next {
            self.maintenance.arm(deadline);
        }
        let count = expired.len();
        self.expired.fetch_add(count as u64, Ordering::Relaxed);
        drop(expired);
        count
    }
    /// Fence request access first so queued callbacks cannot insert again.
    pub(crate) fn clear(&self) {
        let entries = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *entries)
        };
        // A queued callback can retain this map after the route retires. Empty
        // it explicitly, without sending response-finals under the map lock.
        drop(entries);
    }
    pub(crate) fn counts(&self) -> (usize, u64) {
        (
            self.entries.lock().unwrap_or_else(|e| e.into_inner()).len(),
            self.expired.load(Ordering::Relaxed),
        )
    }
}

pub(crate) fn deadline(
    parameter: Option<&str>,
    now: Instant,
) -> Result<Option<Instant>, &'static str> {
    let Some(parameter) = parameter else {
        return Ok(None);
    };
    let millis = parameter
        .parse::<u64>()
        .map_err(|_| "expected milliseconds as an unsigned 64-bit integer")?;
    now.checked_add(Duration::from_millis(millis))
        .map(Some)
        .ok_or("deadline exceeds the monotonic clock range")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_rearms_remaining_deadline_and_stops_after_completion_or_retirement() {
        use futures::FutureExt;

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let maintenance = Arc::new(Maintenance::default());
            let pending = PendingQueries::new(maintenance.clone());
            let now = Instant::now();
            let first_deadline = now - Duration::from_secs(2);
            let second_deadline = now - Duration::from_secs(1);
            let legacy = CddsRequestHeader::create(1, 1);
            let first = CddsRequestHeader::create(1, 2);
            let second = CddsRequestHeader::create(1, 3);
            drop(pending.insert(legacy, Query::empty(), None));
            assert!(maintenance.wait(Instant::now()).now_or_never().is_none());
            drop(pending.insert(first, Query::empty(), Some(first_deadline)));
            drop(pending.insert(second, Query::empty(), Some(second_deadline)));
            tokio::time::timeout(Duration::from_secs(1), maintenance.wait(Instant::now()))
                .await
                .unwrap();
            maintenance.begin_reconcile();
            assert_eq!(pending.expire(first_deadline), 1);
            assert_eq!(pending.counts(), (2, 1));
            tokio::time::timeout(Duration::from_secs(1), maintenance.wait(Instant::now()))
                .await
                .unwrap();

            // Expiring the second deadline leaves only an unknown deadline,
            // which must never arrange periodic scans of its own.
            maintenance.begin_reconcile();
            assert_eq!(pending.expire(now), 1);
            assert!(maintenance.wait(Instant::now()).now_or_never().is_none());
            drop(pending.insert(first, Query::empty(), Some(now)));
            pending.clear();
            // A retired query leaves one stale hint, not recurring maintenance.
            tokio::time::timeout(Duration::from_secs(1), maintenance.wait(Instant::now()))
                .await
                .unwrap();
            maintenance.begin_reconcile();
            assert_eq!(pending.expire(now), 0);
            assert_eq!(pending.counts(), (0, 2));
            assert!(maintenance.wait(Instant::now()).now_or_never().is_none());
        });
    }

    #[test]
    fn reply_correlation_is_independent_of_cdr_byte_order() {
        let pending = PendingQueries::new(Arc::new(Maintenance::default()));
        let le = CddsRequestHeader::from_slice(
            [
                0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba,
                0xdc, 0xfe,
            ],
            true,
        );
        let be = CddsRequestHeader::from_slice(
            [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
                0x32, 0x10,
            ],
            false,
        );
        drop(pending.insert(
            le,
            Query::empty(),
            Some(Instant::now() + Duration::from_secs(1)),
        ));
        assert!(pending.take(&be).is_some());
        assert_eq!(pending.counts(), (0, 0));
    }

    #[test]
    fn only_advertised_deadlines_expire_and_retirement_releases_unknown_deadlines() {
        use std::sync::Arc;

        let pending = Arc::new(PendingQueries::new(Arc::new(Maintenance::default())));
        let queued_callback = pending.clone();
        let now = Instant::now();
        let first = CddsRequestHeader::create(1, 1);
        let second = CddsRequestHeader::create(1, 2);
        let legacy = CddsRequestHeader::create(1, 3);
        drop(pending.insert(first, Query::empty(), Some(now + Duration::from_secs(1))));
        drop(pending.insert(second, Query::empty(), Some(now + Duration::from_secs(300))));
        drop(pending.insert(legacy, Query::empty(), None));
        pending.expire(now + Duration::from_secs(2));
        assert_eq!(pending.counts(), (2, 1));
        assert!(pending.take(&first).is_none());
        assert!(pending.take(&second).is_some());
        pending.expire(now + Duration::from_secs(86400));
        assert_eq!(pending.counts(), (1, 1));
        pending.clear();
        drop(pending);
        assert_eq!(queued_callback.counts(), (0, 1));
        assert!(queued_callback.take(&legacy).is_none());
    }
    #[test]
    fn deadlines_are_either_unknown_explicit_or_invalid_without_a_fallback() {
        let now = Instant::now();
        assert_eq!(deadline(None, now), Ok(None));
        assert_eq!(deadline(Some("0"), now), Ok(Some(now)));
        assert_eq!(
            deadline(Some("300000"), now),
            Ok(Some(now + Duration::from_secs(300)))
        );
        for parameter in ["", "bad", "-1", "18446744073709551616"] {
            assert!(deadline(Some(parameter), now).is_err(), "{parameter}");
        }
        // A valid u64 may exceed Instant's range on some platforms. It must
        // either retain its advertised value or fail, never use another timeout.
        match now.checked_add(Duration::from_millis(u64::MAX)) {
            Some(expected) => assert_eq!(
                deadline(Some("18446744073709551615"), now),
                Ok(Some(expected))
            ),
            None => assert!(deadline(Some("18446744073709551615"), now).is_err()),
        }
    }
}
