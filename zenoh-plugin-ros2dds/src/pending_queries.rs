// Copyright (c) 2026 Logivations
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

//! A query cannot outlive its retention deadline or the route that accepted it.
//! Query destruction can send a Zenoh response-final: always drop outside the lock.
use crate::ros2_utils::CddsRequestHeader;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};
use zenoh::query::Query;

pub(crate) const RETENTION_PARAMETER: &str = "__ros2dds_timeout_ms";

#[derive(Default)]
pub(crate) struct PendingQueries {
    entries: Mutex<HashMap<CddsRequestHeader, (Instant, Query)>>,
    expired: AtomicU64,
}
impl PendingQueries {
    #[must_use = "Drop the replaced query outside any DDS access guard"]
    pub(crate) fn insert(
        &self,
        id: CddsRequestHeader,
        query: Query,
        deadline: Instant,
    ) -> Option<Query> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, (deadline, query))
            .map(|(_, query)| query)
    }
    pub(crate) fn take(&self, id: &CddsRequestHeader) -> Option<Query> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id)
            .map(|(_, query)| query)
    }
    pub(crate) fn expire(&self, now: Instant) -> usize {
        let expired = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            let ids: Vec<_> = entries
                .iter()
                .filter_map(|(id, (deadline, _))| (*deadline <= now).then_some(*id))
                .collect();
            ids.into_iter()
                .filter_map(|id| entries.remove(&id))
                .collect::<Vec<_>>()
        };
        let count = expired.len();
        self.expired.fetch_add(count as u64, Ordering::Relaxed);
        drop(expired);
        count
    }
    pub(crate) fn counts(&self) -> (usize, u64) {
        (
            self.entries.lock().unwrap_or_else(|e| e.into_inner()).len(),
            self.expired.load(Ordering::Relaxed),
        )
    }
}

pub(crate) fn deadline(parameter: Option<&str>, fallback: Duration, now: Instant) -> Instant {
    let retention = parameter
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(fallback);
    now.checked_add(retention).unwrap_or_else(|| now + fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_correlation_is_independent_of_cdr_byte_order() {
        let pending = PendingQueries::default();
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
        drop(pending.insert(le, Query::empty(), Instant::now() + Duration::from_secs(1)));
        assert!(pending.take(&be).is_some());
        assert_eq!(pending.counts(), (0, 0));
    }

    #[test]
    fn only_expired_requests_are_released_without_another_message() {
        let pending = PendingQueries::default();
        let now = Instant::now();
        let first = CddsRequestHeader::create(1, 1);
        let second = CddsRequestHeader::create(1, 2);
        drop(pending.insert(first, Query::empty(), now + Duration::from_secs(1)));
        drop(pending.insert(second, Query::empty(), now + Duration::from_secs(300)));
        pending.expire(now + Duration::from_secs(2));
        assert_eq!(pending.counts(), (1, 1));
        assert!(pending.take(&first).is_none());
        assert!(pending.take(&second).is_some());
        assert_eq!(pending.counts(), (0, 1));
    }
    #[test]
    fn peer_timeout_and_legacy_fallback_have_explicit_retention() {
        let now = Instant::now();
        let fallback = Duration::from_secs(60);
        assert_eq!(deadline(None, fallback, now), now + fallback);
        assert_eq!(deadline(Some("bad"), fallback, now), now + fallback);
        assert_eq!(
            deadline(Some("300000"), fallback, now),
            now + Duration::from_secs(300)
        );
    }
}
