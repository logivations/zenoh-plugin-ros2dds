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

//! Local forwarding observations. These are evidence, not a health verdict:
//! network backpressure and an application waiting to reply are not bridge faults.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use cyclors::{
    dds_create_readcondition, dds_delete, dds_entity_t, dds_triggered, DDS_ALIVE_INSTANCE_STATE,
    DDS_ANY_SAMPLE_STATE, DDS_ANY_VIEW_STATE,
};
use serde::{ser::SerializeMap, Serialize, Serializer};
use zenoh::session::EntityGlobalId;

use crate::{
    dds_utils::{get_guid, AtomicDDSEntity, DDS_ENTITY_NULL},
    gid::Gid,
};

pub(crate) fn serialize_arc<T: Serialize, S: Serializer>(
    value: &Arc<T>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    value.as_ref().serialize(serializer)
}

pub(crate) fn serialize_source_id<S: Serializer>(
    source: &EntityGlobalId,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&source_key(source))
}

fn source_key(source: &EntityGlobalId) -> String {
    format!("{}:{}", source.zid(), source.eid())
}

// Receipt history is only recovery evidence; it must not grow with every bridge
// restart. Eviction loses evidence (an observer must reset its baseline), never
// manufactures a successful delivery. The common case allocates only when a new
// source is first observed; no strings or payloads are retained on the data path.
const MAX_RECEIPT_SOURCES: usize = 1024;

#[derive(Default)]
struct ReceiptCounts {
    sources: HashMap<EntityGlobalId, Receipt>,
    observed: u64,
}

struct Receipt {
    count: u64,
    observed: u64,
}

/// Successful writes to DDS, attributed by native Zenoh source identity.
///
/// Topic SourceInfo and service Reply::replier_id survive routing and timestamp
/// rewriting. Counting only after DDS write succeeds proves that this source's
/// actual data crossed the receiving bridge, even on a topic shared by cameras.
#[derive(Default)]
pub(crate) struct Receipts(Mutex<ReceiptCounts>);

impl Receipts {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn record(&self, source: EntityGlobalId) {
        let mut counts = self.0.lock().unwrap_or_else(|e| e.into_inner());
        counts.observed = counts.observed.saturating_add(1);
        let observed = counts.observed;
        if let Some(receipt) = counts.sources.get_mut(&source) {
            receipt.count = receipt.count.saturating_add(1);
            receipt.observed = observed;
            return;
        }
        if counts.sources.len() == MAX_RECEIPT_SOURCES {
            if let Some(oldest) = counts
                .sources
                .iter()
                .min_by_key(|(_, receipt)| receipt.observed)
                .map(|(source, _)| *source)
            {
                counts.sources.remove(&oldest);
            }
        }
        counts
            .sources
            .insert(source, Receipt { count: 1, observed });
    }
}

impl Serialize for Receipts {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let counts = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let mut map = serializer.serialize_map(Some(counts.sources.len()))?;
        for (source, receipt) in &counts.sources {
            map.serialize_entry(&source_key(source), &receipt.count)?;
        }
        map.end()
    }
}

/// A single local forwarding stage, shared by its route and callbacks.
///
/// A guard covers accepted callback work, including queued work, until its local
/// forwarding call returns. It must never cover waiting for an application reply.
/// Counters survive idle periods, so an external observer can distinguish no
/// traffic, progress, failed forwarding, and work that has not returned.
pub(crate) struct Progress {
    entered: AtomicU64,
    completed: AtomicU64,
    succeeded: AtomicU64,
    in_flight: AtomicU64,
}

impl Progress {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
        })
    }

    pub(crate) fn begin(self: &Arc<Self>) -> ProgressGuard {
        self.entered.fetch_add(1, Ordering::Release);
        self.in_flight.fetch_add(1, Ordering::Release);
        ProgressGuard {
            progress: self.clone(),
            succeeded: false,
        }
    }

    fn snapshot(&self) -> ProgressSnapshot {
        // Load in this order so a concurrent completion cannot make succeeded
        // exceed completed, or completed exceed entered. This is a live sample,
        // not a transaction: observations must persist across admin samples.
        let succeeded = self.succeeded.load(Ordering::Acquire);
        let completed = self.completed.load(Ordering::Acquire);
        let entered = self.entered.load(Ordering::Acquire);
        let in_flight = self.in_flight.load(Ordering::Acquire);
        ProgressSnapshot {
            schema: 1,
            entered,
            completed,
            succeeded,
            in_flight,
        }
    }
}

#[derive(Serialize)]
struct ProgressSnapshot {
    schema: u8,
    entered: u64,
    completed: u64,
    succeeded: u64,
    in_flight: u64,
}

impl Serialize for Progress {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.snapshot().serialize(serializer)
    }
}

/// Movable into a send queue without an allocation. Every exit, including an
/// early return, discarded queued job, or unwinding panic, completes the work.
pub(crate) struct ProgressGuard {
    progress: Arc<Progress>,
    succeeded: bool,
}

impl ProgressGuard {
    pub(crate) fn succeed(&mut self) {
        self.succeeded = true;
    }
}

impl Drop for ProgressGuard {
    fn drop(&mut self) {
        self.progress.completed.fetch_add(1, Ordering::Release);
        if self.succeeded {
            self.progress.succeeded.fetch_add(1, Ordering::Release);
        }
        self.progress.in_flight.fetch_sub(1, Ordering::Release);
    }
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

enum ReaderEntity {
    Atomic(Arc<AtomicDDSEntity>),
    Fixed(dds_entity_t),
}

/// Inspects the existing DataReader only during an admin snapshot. Creating a
/// readcondition adds no subscription and neither reads nor takes any samples.
pub(crate) struct ReaderHealth {
    entity: ReaderEntity,
    read_period: Option<Duration>,
}

impl ReaderHealth {
    pub(crate) fn atomic(entity: Arc<AtomicDDSEntity>, read_period: Option<Duration>) -> Self {
        Self {
            entity: ReaderEntity::Atomic(entity),
            read_period,
        }
    }

    pub(crate) fn fixed(entity: dds_entity_t, read_period: Option<Duration>) -> Self {
        Self {
            entity: ReaderEntity::Fixed(entity),
            read_period,
        }
    }

    fn snapshot(&self) -> ReaderState {
        match &self.entity {
            ReaderEntity::Fixed(entity) => inspect_reader(*entity, self.read_period),
            ReaderEntity::Atomic(entity) => {
                let before = entity.load(Ordering::Acquire);
                let state = inspect_reader(before, self.read_period);
                if entity.load(Ordering::Acquire) == before {
                    state
                } else {
                    ReaderState::unknown(self.read_period)
                }
            }
        }
    }
}

impl Serialize for ReaderHealth {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.snapshot().serialize(serializer)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum ReaderStatus {
    Present,
    Absent,
    Unknown,
}

#[derive(Serialize)]
pub(crate) struct ReaderState {
    state: ReaderStatus,
    guid: Option<Gid>,
    // Available live samples, including samples read but not taken. The bridge
    // uses takecdr; disposal/no-writer notifications are not forwarding work.
    unread: Option<bool>,
    read_period_ms: u64,
}

impl ReaderState {
    fn unknown(read_period: Option<Duration>) -> Self {
        Self {
            state: ReaderStatus::Unknown,
            guid: None,
            unread: None,
            read_period_ms: read_period.map(millis).unwrap_or(0),
        }
    }
}

pub(crate) fn inspect_reader(entity: dds_entity_t, read_period: Option<Duration>) -> ReaderState {
    let mut state = ReaderState::unknown(read_period);
    if entity == DDS_ENTITY_NULL {
        state.state = ReaderStatus::Absent;
        return state;
    }
    let Ok(before) = get_guid(&entity) else {
        return state;
    };
    // These APIs pin/check DDS entities internally. A concurrent route retirement
    // may invalidate any step; never turn its error into an empty-cache verdict.
    let condition = unsafe {
        dds_create_readcondition(
            entity,
            DDS_ANY_SAMPLE_STATE | DDS_ANY_VIEW_STATE | DDS_ALIVE_INSTANCE_STATE,
        )
    };
    if condition <= 0 {
        return state;
    }
    let triggered = unsafe { dds_triggered(condition) };
    let deleted = unsafe { dds_delete(condition) };
    if triggered < 0 || deleted != 0 || get_guid(&entity).ok() != Some(before) {
        return state;
    }
    state.state = ReaderStatus::Present;
    state.guid = Some(before);
    state.unread = Some(triggered != 0);
    state
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_discarded_and_panicking_work_cannot_remain_in_flight() {
        let progress = Progress::new();
        let mut succeeded = progress.begin();
        succeeded.succeed();
        drop(succeeded);
        drop(progress.begin());
        let queued = progress.begin();
        drop(Box::new(move || drop(queued)));
        let _ = std::panic::catch_unwind(|| {
            let _guard = progress.begin();
            panic!("forwarding callback failed");
        });
        let snapshot = progress.snapshot();
        assert_eq!(snapshot.entered, 4);
        assert_eq!(snapshot.completed, 4);
        assert_eq!(snapshot.succeeded, 1);
        assert_eq!(snapshot.in_flight, 0);
    }

    #[test]
    fn completed_call_does_not_hide_another_blocked_call() {
        let progress = Progress::new();
        let held = progress.begin();
        let concurrent = progress.clone();
        std::thread::spawn(move || {
            let mut guard = concurrent.begin();
            guard.succeed();
        })
        .join()
        .unwrap();
        let snapshot = progress.snapshot();
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.in_flight, 1);
        drop(held);
        assert_eq!(progress.snapshot().in_flight, 0);
    }

    #[test]
    fn missing_or_invalid_reader_is_not_reported_drained() {
        for entity in [DDS_ENTITY_NULL, -1, i32::MAX] {
            let snapshot = inspect_reader(entity, Some(Duration::from_secs(3)));
            assert_eq!(snapshot.unread, None);
            assert_eq!(snapshot.read_period_ms, 3000);
        }
    }

    #[test]
    fn another_source_cannot_mask_missing_delivery_from_the_requested_source() {
        let receipts = Receipts::new();
        let camera_a = EntityGlobalId::new("a".parse().unwrap(), 1);
        let camera_b = EntityGlobalId::new("b".parse().unwrap(), 1);
        receipts.record(camera_a);
        for _ in 0..3 {
            receipts.record(camera_b);
        }
        let snapshot = serde_json::to_value(&*receipts).unwrap();
        assert_eq!(snapshot["a:1"], 1);
        assert_eq!(snapshot["b:1"], 3);
        // The same host's replacement publisher is a different source epoch.
        receipts.record(EntityGlobalId::new(camera_a.zid(), 2));
        let snapshot = serde_json::to_value(&*receipts).unwrap();
        assert_eq!(snapshot["a:1"], 1);
        assert_eq!(snapshot["a:2"], 1);
    }

    #[test]
    fn source_churn_cannot_grow_receipts_or_inherit_an_old_delivery_count() {
        let receipts = Receipts::new();
        let zid = "a".parse().unwrap();
        for eid in 0..MAX_RECEIPT_SOURCES as u32 {
            receipts.record(EntityGlobalId::new(zid, eid));
        }
        // Keep the first source active while new bridge epochs appear.
        receipts.record(EntityGlobalId::new(zid, 0));
        receipts.record(EntityGlobalId::new(zid, MAX_RECEIPT_SOURCES as u32));
        let snapshot = serde_json::to_value(&*receipts).unwrap();
        assert_eq!(snapshot.as_object().unwrap().len(), MAX_RECEIPT_SOURCES);
        assert_eq!(snapshot["a:0"], 2);
        assert!(snapshot.get("a:1").is_none());
        receipts.record(EntityGlobalId::new(zid, 1));
        let snapshot = serde_json::to_value(&*receipts).unwrap();
        assert_eq!(snapshot["a:1"], 1);
        assert_eq!(snapshot.as_object().unwrap().len(), MAX_RECEIPT_SOURCES);
    }
}
