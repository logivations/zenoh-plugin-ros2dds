// Copyright (c) 2026 Logivations
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

//! Route-owned DDS resources. Data callbacks borrow access, never ownership.
use cyclors::{
    qos::{History, HistoryKind, Qos},
    *,
};
use serde::{Serialize, Serializer};
use std::{
    ffi::CStr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};
use tokio::task::JoinHandle;

use crate::{
    dds_types::{DDSRawSample, TypeInfo},
    dds_utils::{create_topic, delete_dds_entity, get_guid},
    gid::Gid,
    ros_discovery::RosDiscoveryInfoMgr,
};

pub(crate) static LIVE_ENDPOINTS: AtomicUsize = AtomicUsize::new(0);
pub(crate) static LIVE_TOPICS: AtomicUsize = AtomicUsize::new(0);
pub(crate) static LIVE_LISTENERS: AtomicUsize = AtomicUsize::new(0);
pub(crate) static CLEANUP_FAILURES: AtomicUsize = AtomicUsize::new(0);

struct Topic(dds_entity_t);
impl Drop for Topic {
    fn drop(&mut self) {
        match delete_dds_entity(self.0) {
            Ok(()) => {
                LIVE_TOPICS.fetch_sub(1, Ordering::Relaxed);
            }
            Err(error) => quarantine(error),
        }
    }
}

/// A revocable borrow for an in-flight data operation. Closing waits for current
/// users, then releases the lock BEFORE DDS deletion/callback drainage. A queued
/// callback keeps only this empty cell after retirement, not a reusable raw ID.
#[derive(Clone)]
pub(crate) struct DdsAccess(Arc<RwLock<Option<dds_entity_t>>>);
impl DdsAccess {
    fn new(entity: dds_entity_t) -> Self {
        Self(Arc::new(RwLock::new(Some(entity))))
    }
    pub(crate) fn with<R>(&self, f: impl FnOnce(dds_entity_t) -> R) -> Option<R> {
        let guard = self.0.read().unwrap_or_else(|e| e.into_inner());
        guard.map(f)
    }
    pub(crate) fn close(&self) {
        self.0.write().unwrap_or_else(|e| e.into_inner()).take();
    }
}

struct Listener<F> {
    enabled: Arc<AtomicBool>,
    callback: F,
}
struct ListenerArg {
    ptr: usize,
    release: unsafe fn(usize),
    enabled: Arc<AtomicBool>,
}
impl Drop for ListenerArg {
    fn drop(&mut self) {
        unsafe { (self.release)(self.ptr) }
        LIVE_LISTENERS.fetch_sub(1, Ordering::Relaxed);
    }
}
unsafe fn release_listener<F>(ptr: usize) {
    drop(Box::from_raw(ptr as *mut Listener<F>));
}

unsafe extern "C" fn on_data<F>(reader: dds_entity_t, arg: *mut std::ffi::c_void)
where
    F: Fn(&DDSRawSample) + Send + Sync + 'static,
{
    let listener = &*(arg as *const Listener<F>);
    while listener.enabled.load(Ordering::Acquire) {
        match take_sample(reader) {
            Some(sample) => (listener.callback)(&sample),
            None => break,
        }
    }
}

fn take_sample(reader: dds_entity_t) -> Option<DDSRawSample> {
    unsafe {
        let mut data: *mut ddsi_serdata = std::ptr::null_mut();
        let mut info = std::mem::MaybeUninit::<dds_sample_info_t>::uninit();
        while dds_takecdr(reader, &mut data, 1, info.as_mut_ptr(), DDS_ANY_STATE) > 0 {
            // DDSRawSample retains its own serdata reference, independent of
            // the reader and the reference returned by takecdr.
            let sample = info
                .assume_init()
                .valid_data
                .then(|| DDSRawSample::create(data));
            ddsi_serdata_unref(data);
            if sample.is_some() {
                return sample;
            }
        }
        None
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Reader,
    Writer,
}

pub(crate) struct DdsEndpoint {
    entity: dds_entity_t,
    topic: Option<Topic>,
    gid: Gid,
    kind: Kind,
    access: DdsAccess,
    enabled: Arc<AtomicBool>,
    listener: Option<ListenerArg>,
    poll: Option<JoinHandle<()>>,
    advertised: Option<Arc<RosDiscoveryInfoMgr>>,
}

impl Serialize for DdsEndpoint {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        crate::dds_utils::serialize_entity_guid(&self.entity, s)
    }
}

pub(crate) fn serialize_optional<S: Serializer>(
    endpoint: &Option<DdsEndpoint>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match endpoint {
        Some(endpoint) => endpoint.serialize(s),
        None => s.serialize_str(""),
    }
}

fn dds_error(stage: &str, code: i32) -> String {
    let message = unsafe { CStr::from_ptr(dds_strretcode(code)).to_string_lossy() };
    format!("{stage}: {message} ({code})")
}

impl DdsEndpoint {
    pub(crate) fn writer(
        participant: i32,
        topic: String,
        typ: String,
        keyless: bool,
        mut qos: Qos,
    ) -> Result<Self, String> {
        // An in-flight write holds revocable access. Infinite DDS backpressure
        // would prevent retirement from ever acquiring the fence. Use the DDS
        // writer default (100 ms) as an upper bound, preserving stricter limits.
        // max_blocking_time does not participate in DDS reliability matching.
        if let Some(reliability) = &mut qos.reliability {
            reliability.max_blocking_time = reliability.max_blocking_time.min(100_000_000);
        }
        Self::create(
            participant,
            topic,
            typ,
            &None,
            keyless,
            qos,
            Kind::Writer,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reader<F>(
        participant: i32,
        topic: String,
        typ: String,
        type_info: &Option<Arc<TypeInfo>>,
        keyless: bool,
        mut qos: Qos,
        period: Option<Duration>,
        callback: F,
    ) -> Result<Self, String>
    where
        F: Fn(&DDSRawSample) + Send + Sync + 'static,
    {
        let enabled = Arc::new(AtomicBool::new(true));
        if let Some(period) = period {
            qos.history = Some(History {
                kind: HistoryKind::KEEP_LAST,
                depth: 1,
            });
            let mut endpoint = Self::create(
                participant,
                topic,
                typ,
                type_info,
                keyless,
                qos,
                Kind::Reader,
                None,
                None,
            )?;
            endpoint.enabled = enabled.clone();
            let access = endpoint.access();
            endpoint.poll = Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(period).await;
                    if !enabled.load(Ordering::Acquire) {
                        break;
                    }
                    // KEEP_LAST(1): take one owned sample under the guard. Never
                    // hold a lifecycle borrow while forwarding over Zenoh.
                    match access.with(take_sample) {
                        Some(Some(sample)) if enabled.load(Ordering::Acquire) => callback(&sample),
                        None => break,
                        _ => {}
                    }
                }
            }));
            Ok(endpoint)
        } else {
            let arg = Box::new(Listener {
                enabled: enabled.clone(),
                callback,
            });
            let arg = ListenerArg {
                ptr: Box::into_raw(arg) as usize,
                release: release_listener::<F>,
                enabled: enabled.clone(),
            };
            LIVE_LISTENERS.fetch_add(1, Ordering::Relaxed);
            let mut endpoint = Self::create(
                participant,
                topic,
                typ,
                type_info,
                keyless,
                qos,
                Kind::Reader,
                Some(arg),
                Some(on_data::<F>),
            )?;
            endpoint.enabled = enabled;
            Ok(endpoint)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create(
        participant: i32,
        topic_name: String,
        typ: String,
        type_info: &Option<Arc<TypeInfo>>,
        keyless: bool,
        qos: Qos,
        kind: Kind,
        listener_arg: Option<ListenerArg>,
        callback: Option<unsafe extern "C" fn(dds_entity_t, *mut std::ffi::c_void)>,
    ) -> Result<Self, String> {
        if CLEANUP_FAILURES.load(Ordering::Acquire) != 0 {
            return Err(
                "DDS cleanup failed; endpoint state is quarantined until bridge recovery".into(),
            );
        }
        #[cfg(any(test, feature = "lifecycle-test-hooks"))]
        fault::checkpoint("before topic")?;
        let topic = unsafe { create_topic(participant, &topic_name, &typ, type_info, keyless) };
        if topic <= 0 {
            return Err(dds_error("create topic", topic));
        }
        LIVE_TOPICS.fetch_add(1, Ordering::Relaxed);
        let topic = Topic(topic);
        #[cfg(any(test, feature = "lifecycle-test-hooks"))]
        fault::checkpoint("after topic")?;
        let entity = unsafe {
            let listener = listener_arg.as_ref().map(|arg| {
                let l = dds_create_listener(arg.ptr as *mut std::ffi::c_void);
                dds_lset_data_available(l, callback);
                l
            });
            let qos_native = qos.to_qos_native();
            let entity = match kind {
                Kind::Reader => dds_create_reader(
                    participant,
                    topic.0,
                    qos_native,
                    listener.unwrap_or(std::ptr::null_mut()),
                ),
                Kind::Writer => {
                    dds_create_writer(participant, topic.0, qos_native, std::ptr::null())
                }
            };
            Qos::delete_qos_native(qos_native);
            // DDS copies the listener. The callback argument belongs to us until
            // dds_delete has drained callbacks, not to dds_listener_t.
            if let Some(listener) = listener {
                dds_delete_listener(listener);
            }
            entity
        };
        if entity <= 0 {
            return Err(dds_error("create endpoint", entity));
        }
        LIVE_ENDPOINTS.fetch_add(1, Ordering::Relaxed);
        let mut endpoint = Self {
            entity,
            topic: Some(topic),
            gid: Gid::NOT_DISCOVERED,
            kind,
            access: DdsAccess::new(entity),
            enabled: listener_arg
                .as_ref()
                .map(|a| a.enabled.clone())
                .unwrap_or_else(|| Arc::new(AtomicBool::new(true))),
            listener: listener_arg,
            poll: None,
            advertised: None,
        };
        #[cfg(any(test, feature = "lifecycle-test-hooks"))]
        fault::checkpoint("after endpoint")?;
        endpoint.gid = get_guid(&entity)?;
        #[cfg(any(test, feature = "lifecycle-test-hooks"))]
        fault::checkpoint("after guid")?;
        Ok(endpoint)
    }

    pub(crate) fn entity(&self) -> i32 {
        self.entity
    }

    #[cfg(feature = "lifecycle-test-hooks")]
    pub(crate) fn invalidate_for_test(&self) -> Result<(), String> {
        // Intentionally violate ownership to test independent health detection.
        // Leave the owner's handle unchanged. This build is never deployable.
        delete_dds_entity(self.entity)
    }
    pub(crate) fn access(&self) -> DdsAccess {
        self.access.clone()
    }
    pub(crate) fn advertise(&mut self, graph: Arc<RosDiscoveryInfoMgr>) {
        if self.advertised.is_none() {
            match self.kind {
                Kind::Reader => graph.add_dds_reader(self.gid),
                Kind::Writer => graph.add_dds_writer(self.gid),
            }
            self.advertised = Some(graph);
        }
    }
    pub(crate) fn advertise_pair(
        reader: &mut Self,
        writer: &mut Self,
        graph: Arc<RosDiscoveryInfoMgr>,
    ) {
        assert!(matches!(reader.kind, Kind::Reader) && matches!(writer.kind, Kind::Writer));
        assert!(reader.advertised.is_none() && writer.advertised.is_none());
        graph.add_dds_pair(reader.gid, writer.gid);
        reader.advertised = Some(graph.clone());
        writer.advertised = Some(graph);
    }
    pub(crate) fn fence(&self) {
        self.enabled.store(false, Ordering::Release);
        self.access.close();
    }

    pub(crate) fn withdraw_pair(reader: &mut Self, writer: &mut Self) {
        if let Some(graph) = reader.advertised.take() {
            graph.remove_dds_pair(reader.gid, writer.gid);
            writer.advertised = None;
        }
    }
}

fn quarantine(error: String) {
    CLEANUP_FAILURES.fetch_add(1, Ordering::Release);
    tracing::error!(
        "DDS cleanup failed; refusing new route endpoints until bridge recovery: {error}"
    );
}

impl Drop for DdsEndpoint {
    fn drop(&mut self) {
        self.fence();
        if let Some(task) = self.poll.take() {
            task.abort();
        }
        if let Some(graph) = self.advertised.take() {
            match self.kind {
                Kind::Reader => graph.remove_dds_reader(self.gid),
                Kind::Writer => graph.remove_dds_writer(self.gid),
            }
        }
        // No access lock is held here. DDS deletion drains the data callback.
        // On an unexpected DDS internal error, freeing its argument would be a
        // use-after-free. Quarantine it, latch health failure and prohibit reuse.
        if let Err(error) = delete_dds_entity(self.entity) {
            if let Some(listener) = self.listener.take() {
                std::mem::forget(listener);
            }
            if let Some(topic) = self.topic.take() {
                std::mem::forget(topic);
            }
            quarantine(error);
            return;
        }
        self.listener.take();
        LIVE_ENDPOINTS.fetch_sub(1, Ordering::Relaxed);
        self.topic.take();
    }
}

#[cfg(any(test, feature = "lifecycle-test-hooks"))]
pub(crate) mod fault {
    use std::sync::atomic::{AtomicIsize, Ordering};
    static FAIL_AT: AtomicIsize = AtomicIsize::new(-1);
    pub(crate) fn fail_after(n: usize) {
        FAIL_AT.store(n as isize, Ordering::SeqCst);
    }
    pub(crate) fn checkpoint(stage: &str) -> Result<(), String> {
        #[cfg(feature = "lifecycle-test-hooks")]
        crate::lifecycle_test_hooks::creation_checkpoint(stage)?;
        match FAIL_AT.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
            (n >= 0).then_some(n - 1)
        }) {
            Ok(0) => Err(format!("injected failure: {stage}")),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Barrier};

    static DDS_TEST: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn every_pair_creation_failure_releases_real_dds_resources() {
        let _serial = DDS_TEST.lock().unwrap();
        let config = std::ffi::CString::new("<CycloneDDS><Domain><General><Interfaces><NetworkInterface address='127.0.0.1'/></Interfaces><AllowMulticast>false</AllowMulticast></General><Discovery><ParticipantIndex>none</ParticipantIndex></Discovery></Domain></CycloneDDS>").unwrap();
        let domain = unsafe { dds_create_domain(219, config.as_ptr()) };
        assert!(domain > 0, "domain: {domain}");
        let participant =
            unsafe { dds_create_participant(219, std::ptr::null(), std::ptr::null()) };
        assert!(participant > 0);
        let children = || unsafe { dds_get_children(participant, std::ptr::null_mut(), 0) };
        let baseline = children();
        let build_pair = || -> Result<(DdsEndpoint, DdsEndpoint), String> {
            let writer = DdsEndpoint::writer(
                participant,
                "rr/testReply".into(),
                "TestReply".into(),
                true,
                Qos::default(),
            )?;
            let reader = DdsEndpoint::reader(
                participant,
                "rq/testRequest".into(),
                "TestRequest".into(),
                &None,
                true,
                Qos::default(),
                None,
                |_| {},
            )?;
            Ok((reader, writer))
        };
        // Four real construction boundaries on each side of the pair.
        for stage in 0..8 {
            fault::fail_after(stage);
            assert!(build_pair().err().unwrap().contains("injected failure"));
            assert_eq!(children(), baseline, "DDS child leak at stage {stage}");
            assert_eq!(LIVE_ENDPOINTS.load(Ordering::Relaxed), 0);
            assert_eq!(LIVE_TOPICS.load(Ordering::Relaxed), 0);
            assert_eq!(LIVE_LISTENERS.load(Ordering::Relaxed), 0);
        }
        for _ in 0..100 {
            drop(build_pair().unwrap());
        }
        assert_eq!(children(), baseline);
        assert_eq!(CLEANUP_FAILURES.load(Ordering::Relaxed), 0);
        delete_dds_entity(participant).unwrap();
        delete_dds_entity(domain).unwrap();
    }

    #[test]
    fn real_dds_deletion_drains_callback_before_releasing_its_argument() {
        let _serial = DDS_TEST.lock().unwrap();
        let config = std::ffi::CString::new("<CycloneDDS><Domain><General><Interfaces><NetworkInterface address='127.0.0.1'/></Interfaces><AllowMulticast>false</AllowMulticast></General><Discovery><ParticipantIndex>none</ParticipantIndex></Discovery></Domain></CycloneDDS>").unwrap();
        let domain = unsafe { dds_create_domain(220, config.as_ptr()) };
        let participant =
            unsafe { dds_create_participant(220, std::ptr::null(), std::ptr::null()) };
        assert!(participant > 0 && domain > 0);
        let writer = DdsEndpoint::writer(
            participant,
            "rt/drain".into(),
            "Raw".into(),
            true,
            Qos::default(),
        )
        .unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new(Barrier::new(2));
        let release_callback = release.clone();
        let reader = DdsEndpoint::reader(
            participant,
            "rt/drain".into(),
            "Raw".into(),
            &None,
            true,
            Qos::default(),
            None,
            move |_| {
                entered_tx.send(()).unwrap();
                release_callback.wait();
            },
        )
        .unwrap();
        let access = writer.access();
        let writing = std::thread::spawn(move || {
            access
                .with(|entity| {
                    crate::dds_utils::dds_write(
                        entity,
                        vec![0, 1, 0, 0, 4, 0, 0, 0, b'f', b'o', b'o', 0],
                    )
                })
                .unwrap()
                .unwrap()
        });
        entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let (retired_tx, retired_rx) = mpsc::channel();
        let retiring = std::thread::spawn(move || {
            drop(reader);
            retired_tx.send(()).unwrap();
        });
        assert!(retired_rx.recv_timeout(Duration::from_millis(20)).is_err());
        assert_eq!(
            LIVE_LISTENERS.load(Ordering::Relaxed),
            1,
            "callback argument freed while in use"
        );
        release.wait();
        retired_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        retiring.join().unwrap();
        writing.join().unwrap();
        drop(writer);
        assert_eq!(LIVE_LISTENERS.load(Ordering::Relaxed), 0);
        assert_eq!(LIVE_ENDPOINTS.load(Ordering::Relaxed), 0);
        assert_eq!(LIVE_TOPICS.load(Ordering::Relaxed), 0);
        delete_dds_entity(participant).unwrap();
        delete_dds_entity(domain).unwrap();
    }
    #[test]
    fn retirement_waits_for_borrow_then_rejects_delayed_callback() {
        let access = DdsAccess::new(42);
        let held = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let worker = {
            let (a, h, r) = (access.clone(), held.clone(), release.clone());
            std::thread::spawn(move || {
                a.with(|entity| {
                    assert_eq!(entity, 42);
                    h.wait();
                    r.wait();
                })
            })
        };
        held.wait();
        let (done_tx, done_rx) = mpsc::channel();
        let retire = {
            let a = access.clone();
            std::thread::spawn(move || {
                a.close();
                done_tx.send(()).unwrap();
            })
        };
        assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());
        release.wait();
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        worker.join().unwrap();
        retire.join().unwrap();
        assert!(access.with(|_| panic!("stale raw handle used")).is_none());
    }

    #[test]
    fn stalled_reliable_reader_cannot_block_owned_writer_retirement() {
        use cyclors::qos::{Reliability, ReliabilityKind, ResourceLimits, DDS_INFINITE_TIME};
        let _serial = DDS_TEST.lock().unwrap();
        let config = std::ffi::CString::new("<CycloneDDS><Domain><General><Interfaces><NetworkInterface address='127.0.0.1'/></Interfaces><AllowMulticast>false</AllowMulticast></General><Discovery><ParticipantIndex>none</ParticipantIndex></Discovery></Domain></CycloneDDS>").unwrap();
        let domain = unsafe { dds_create_domain(221, config.as_ptr()) };
        let participant =
            unsafe { dds_create_participant(221, std::ptr::null(), std::ptr::null()) };
        assert!(participant > 0 && domain > 0);
        let qos = Qos {
            history: Some(History {
                kind: HistoryKind::KEEP_ALL,
                depth: 0,
            }),
            reliability: Some(Reliability {
                kind: ReliabilityKind::RELIABLE,
                max_blocking_time: DDS_INFINITE_TIME,
            }),
            resource_limits: Some(ResourceLimits {
                max_samples: 1,
                max_instances: 1,
                max_samples_per_instance: 1,
            }),
            ..Qos::default()
        };
        let writer = DdsEndpoint::writer(
            participant,
            "rt/backpressure".into(),
            "Raw".into(),
            true,
            qos.clone(),
        )
        .unwrap();
        let native_qos = unsafe { qos.to_qos_native() };
        let reader = unsafe {
            dds_create_reader(
                participant,
                writer.topic.as_ref().unwrap().0,
                native_qos,
                std::ptr::null(),
            )
        };
        unsafe { Qos::delete_qos_native(native_qos) };
        assert!(reader > 0);
        let sample = vec![0, 1, 0, 0, 4, 0, 0, 0, b'f', b'o', b'o', 0];
        crate::dds_utils::dds_write(writer.entity, sample.clone()).unwrap();
        let access = writer.access();
        let (entered_tx, entered_rx) = mpsc::channel();
        let writing = std::thread::spawn(move || {
            access.with(|entity| {
                entered_tx.send(()).unwrap();
                crate::dds_utils::dds_write(entity, sample)
            })
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let retiring = std::thread::spawn(move || {
            drop(writer);
            done_tx.send(()).unwrap();
        });
        let bounded = done_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        // Drain the deliberate backpressure before asserting. Deleting the
        // reader itself would also wait on Cyclone's local-delivery retry lock.
        drop(take_sample(reader));
        let result = writing.join().unwrap();
        retiring.join().unwrap();
        delete_dds_entity(reader).unwrap();
        delete_dds_entity(participant).unwrap();
        delete_dds_entity(domain).unwrap();
        assert!(
            bounded,
            "an unbounded DDS write held the retirement fence until the external reader drained"
        );
        assert!(
            matches!(result, Some(Err(_))),
            "full reliable reader should time out explicitly"
        );
    }
}
