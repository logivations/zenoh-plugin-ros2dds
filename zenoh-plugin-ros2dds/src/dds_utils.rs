//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::{
    ffi::{CStr, CString},
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicBool, AtomicI32, Ordering},
        Arc, Mutex, MutexGuard, PoisonError,
    },
    time::Duration,
};

use cyclors::{
    qos::{History, HistoryKind, Qos},
    *,
};
use serde::Serializer;
use tokio::task;

use crate::{
    dds_types::{DDSRawSample, TypeInfo},
    gid::Gid,
    vec_into_raw_parts,
};

// An atomic dds_entity_t (=i32), for safe concurrent creation/deletion of DDS entities
pub type AtomicDDSEntity = AtomicI32;

/// Serializes a route's DDS entity activation (performed from a zenoh
/// `MatchingListener` callback) with the route's retirement (`Drop`).
///
/// Taking the `MatchingListener` in `Drop` is not a completion barrier: a
/// callback that already started keeps executing after the listener handle is
/// dropped, and can re-create a DDS entity for the retired route - a permanent
/// orphan, since no further matching event can ever delete it. A plain atomic
/// flag is not enough either: it leaves a window between the flag check and
/// the entity creation. Instead, the callback checks liveness and creates the
/// entities inside a single critical section (`if_live`), and `retire()` takes
/// the same lock before clearing the flag. Once `retire()` returns, any
/// in-flight activation has fully completed and no later callback can create
/// entities anymore, so the deactivation that follows in `Drop` is final.
///
/// Locking discipline (cf. eclipse-zenoh/zenoh-plugin-ros2dds#382): `Drop`
/// must drop the `MatchingListener` handle only after `retire()` has released
/// this lock, since undeclaring the listener may wait for an in-flight
/// callback that could itself be blocked on this lock.
pub struct RouteLiveness {
    live: Mutex<bool>,
    // whether the poisoned-lock recovery was already logged for this route
    poison_logged: AtomicBool,
}

impl RouteLiveness {
    pub fn new() -> Arc<Self> {
        Arc::new(RouteLiveness {
            live: Mutex::new(true),
            poison_logged: AtomicBool::new(false),
        })
    }

    /// Runs `op` holding the liveness lock if the route was not retired yet.
    /// Returns `None` (without running `op`) after `retire()`.
    pub fn if_live<R>(&self, op: impl FnOnce() -> R) -> Option<R> {
        let guard = self.lock();
        if *guard {
            Some(op())
        } else {
            None
        }
    }

    /// Marks the route retired. Blocks until no `if_live` section is running;
    /// afterwards `if_live` never runs its closure again.
    pub fn retire(&self) {
        *self.lock() = false;
    }

    fn lock(&self) -> MutexGuard<'_, bool> {
        // A poisoned lock only means some closure panicked mid-activation;
        // the liveness flag itself remains meaningful, so recover and
        // continue - but say so once at error level, since the panicking
        // closure may have left a partial activation behind.
        self.live.lock().unwrap_or_else(|poison| {
            if !self.poison_logged.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    "route liveness lock poisoned by a panic in an activation/deactivation closure; recovering and continuing with the current liveness state"
                );
            }
            PoisonError::into_inner(poison)
        })
    }
}

pub const DDS_ENTITY_NULL: dds_entity_t = 0;
pub const CDR_HEADER_LE: [u8; 4] = [0, 1, 0, 0];
pub const CDR_HEADER_BE: [u8; 4] = [0, 0, 0, 0];

/// Return None if the buffer is shorter than a CDR header (4 bytes).
/// Otherwise, return true if the encoding flag (last bit of 2nd byte) corresponds little endian
pub fn is_cdr_little_endian(cdr_buffer: &[u8]) -> Option<bool> {
    // Per DDSI spec §10.2 (https://www.omg.org/spec/DDSI-RTPS/2.5/PDF),
    // the endianness flag is the last bit of the RepresentationOptions (2 last octets)
    if cdr_buffer.len() > 3 {
        Some(cdr_buffer[1] & 1 > 0)
    } else {
        None
    }
}

pub fn ddsrt_iov_len_to_usize(len: ddsrt_iov_len_t) -> Result<usize, String> {
    // Depending the platform ddsrt_iov_len_t can have different typedef
    // See https://github.com/eclipse-cyclonedds/cyclonedds/blob/master/src/ddsrt/include/dds/ddsrt/iovec.h
    // Thus this conversion is NOT useless on Windows where ddsrt_iov_len_t is a u32 !
    #[allow(clippy::useless_conversion)]
    len.try_into()
        .map_err(|e| format!("INTERNAL ERROR converting a ddsrt_iov_len_t to usize: {e}"))
}

pub fn ddsrt_iov_len_from_usize(len: usize) -> Result<ddsrt_iov_len_t, String> {
    // Depending the platform ddsrt_iov_len_t can have different typedef
    // See https://github.com/eclipse-cyclonedds/cyclonedds/blob/master/src/ddsrt/include/dds/ddsrt/iovec.h
    // Thus this conversion is NOT useless on Windows where ddsrt_iov_len_t is a u32 !
    #[allow(clippy::useless_conversion)]
    len.try_into()
        .map_err(|e| format!("INTERNAL ERROR converting a usize to ddsrt_iov_len_t: {e}"))
}

pub fn delete_dds_entity(entity: dds_entity_t) -> Result<(), String> {
    unsafe {
        let r = dds_delete(entity);
        match r {
            0 | DDS_RETCODE_ALREADY_DELETED => Ok(()),
            e => Err(format!("Error deleting DDS entity - retcode={e}")),
        }
    }
}

pub fn get_guid(entity: &dds_entity_t) -> Result<Gid, String> {
    unsafe {
        let mut guid = dds_guid_t { v: [0; 16] };
        let r = dds_get_guid(*entity, &mut guid);
        if r == 0 {
            Ok(Gid::from(guid.v))
        } else {
            Err(format!("Error getting GUID of DDS entity - retcode={r}"))
        }
    }
}

pub fn serialize_entity_guid<S>(entity: &dds_entity_t, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match get_guid(entity) {
        Ok(guid) => s.serialize_str(&guid.to_string()),
        Err(_) => s.serialize_str("UNKOWN_GUID"),
    }
}

pub fn serialize_atomic_entity_guid<S>(entity: &AtomicDDSEntity, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match entity.load(std::sync::atomic::Ordering::Relaxed) {
        DDS_ENTITY_NULL => s.serialize_str(""),
        entity => serialize_entity_guid(&entity, s),
    }
}

pub fn get_instance_handle(entity: dds_entity_t) -> Result<dds_instance_handle_t, String> {
    unsafe {
        let mut handle: dds_instance_handle_t = 0;
        let ret = dds_get_instance_handle(entity, &mut handle);
        if ret == 0 {
            Ok(handle)
        } else {
            Err(format!(
                "falied to get instance handle: {}",
                CStr::from_ptr(dds_strretcode(-ret))
                    .to_str()
                    .unwrap_or("unrecoverable DDS retcode")
            ))
        }
    }
}

pub unsafe fn create_topic(
    dp: dds_entity_t,
    topic_name: &str,
    type_name: &str,
    type_info: &Option<Arc<TypeInfo>>,
    keyless: bool,
) -> dds_entity_t {
    let cton = CString::new(topic_name.to_owned()).unwrap().into_raw();
    let ctyn = CString::new(type_name.to_owned()).unwrap().into_raw();

    let topic = match type_info {
        None => cdds_create_blob_topic(dp, cton, ctyn, keyless),
        Some(type_info) => {
            let mut descriptor: *mut dds_topic_descriptor_t = std::ptr::null_mut();

            let ret = dds_create_topic_descriptor(
                dds_find_scope_DDS_FIND_SCOPE_GLOBAL,
                dp,
                type_info.ptr,
                500000000,
                &mut descriptor,
            );
            let mut topic: dds_entity_t = 0;
            if ret == (DDS_RETCODE_OK as i32) {
                topic = dds_create_topic(dp, descriptor, cton, std::ptr::null(), std::ptr::null());
                assert!(topic >= 0);
                dds_delete_topic_descriptor(descriptor);
            }
            topic
        }
    };

    // Reclaim the CStrings: cyclonedds copies them internally, so it is safe to free now.
    drop(CString::from_raw(cton));
    drop(CString::from_raw(ctyn));

    topic
}

pub fn create_dds_writer(
    dp: dds_entity_t,
    topic_name: String,
    type_name: String,
    keyless: bool,
    qos: Qos,
) -> Result<dds_entity_t, String> {
    let cton = CString::new(topic_name).unwrap().into_raw();
    let ctyn = CString::new(type_name).unwrap().into_raw();

    unsafe {
        let t = cdds_create_blob_topic(dp, cton, ctyn, keyless);
        let qos_native = qos.to_qos_native();
        let writer: i32 = dds_create_writer(dp, t, qos_native, std::ptr::null_mut());
        Qos::delete_qos_native(qos_native);
        drop(CString::from_raw(cton));
        drop(CString::from_raw(ctyn));
        if writer >= 0 {
            Ok(writer)
        } else {
            Err(format!(
                "Error creating DDS Writer: {}",
                CStr::from_ptr(dds_strretcode(-writer))
                    .to_str()
                    .unwrap_or("unrecoverable DDS retcode")
            ))
        }
    }
}

pub fn dds_write(data_writer: dds_entity_t, data: Vec<u8>) -> Result<(), String> {
    unsafe {
        // As per the Vec documentation (see https://doc.rust-lang.org/std/vec/struct.Vec.html#method.into_raw_parts)
        // the only way to correctly releasing it is to create a vec using from_raw_parts
        // and then have its destructor do the cleanup.
        // Thus, while tempting to just pass the raw pointer to cyclone and then free it from C,
        // that is not necessarily safe or guaranteed to be leak free.
        // TODO replace when stable https://github.com/rust-lang/rust/issues/65816
        let (ptr, len, capacity) = vec_into_raw_parts(data);
        let size: ddsrt_iov_len_t = ddsrt_iov_len_from_usize(len)?;

        let data_out = ddsrt_iovec_t {
            iov_base: ptr as *mut std::ffi::c_void,
            iov_len: size,
        };

        let mut sertype_ptr: *const ddsi_sertype = std::ptr::null_mut();
        let ret = dds_get_entity_sertype(data_writer, &mut sertype_ptr);
        if ret < 0 {
            drop(Vec::from_raw_parts(ptr, len, capacity));
            return Err(format!(
                "DDS write failed: sertype lookup failed ({})",
                CStr::from_ptr(dds_strretcode(ret))
                    .to_str()
                    .unwrap_or("unrecoverable DDS retcode")
            ));
        }

        let fwdp = ddsi_serdata_from_ser_iov(
            sertype_ptr,
            ddsi_serdata_kind_SDK_DATA,
            1,
            &data_out,
            size as usize,
        );

        let ret = dds_writecdr(data_writer, fwdp);
        if ret < 0 {
            drop(Vec::from_raw_parts(ptr, len, capacity));
            return Err(format!(
                "DDS write failed: {}",
                CStr::from_ptr(dds_strretcode(ret))
                    .to_str()
                    .unwrap_or("unrecoverable DDS retcode")
            ));
        }

        drop(Vec::from_raw_parts(ptr, len, capacity));
        Ok(())
    }
}

unsafe extern "C" fn listener_to_callback<F>(dr: dds_entity_t, arg: *mut std::os::raw::c_void)
where
    F: Fn(&DDSRawSample),
{
    let callback = arg as *mut F;
    let mut zp: *mut ddsi_serdata = std::ptr::null_mut();
    #[allow(clippy::uninit_assumed_init)]
    let mut si = MaybeUninit::<[dds_sample_info_t; 1]>::uninit();
    while dds_takecdr(
        dr,
        &mut zp,
        1,
        si.as_mut_ptr() as *mut dds_sample_info_t,
        DDS_ANY_STATE,
    ) > 0
    {
        let si = si.assume_init();
        if si[0].valid_data {
            let raw_sample = DDSRawSample::create(zp);

            (*callback)(&raw_sample);
        }
        ddsi_serdata_unref(zp);
    }
}

#[allow(clippy::too_many_arguments)]
pub fn create_dds_reader<F>(
    dp: dds_entity_t,
    topic_name: String,
    type_name: String,
    type_info: &Option<Arc<TypeInfo>>,
    keyless: bool,
    mut qos: Qos,
    read_period: Option<Duration>,
    callback: F,
) -> Result<dds_entity_t, String>
where
    F: Fn(&DDSRawSample) + std::marker::Send + 'static,
{
    unsafe {
        let t = create_topic(dp, &topic_name, &type_name, type_info, keyless);
        match read_period {
            None => {
                // Use a Listener to route data as soon as it arrives
                let arg = Box::new(callback);
                let sub_listener =
                    dds_create_listener(Box::into_raw(arg) as *mut std::os::raw::c_void);
                dds_lset_data_available(sub_listener, Some(listener_to_callback::<F>));
                let qos_native = qos.to_qos_native();
                let reader = dds_create_reader(dp, t, qos_native, sub_listener);
                Qos::delete_qos_native(qos_native);
                if reader >= 0 {
                    let res = dds_reader_wait_for_historical_data(reader, qos::DDS_100MS_DURATION);
                    if res < 0 {
                        tracing::error!(
                            "Error calling dds_reader_wait_for_historical_data(): {}",
                            CStr::from_ptr(dds_strretcode(-res))
                                .to_str()
                                .unwrap_or("unrecoverable DDS retcode")
                        );
                    }
                    Ok(reader)
                } else {
                    Err(format!(
                        "Error creating DDS Reader: {}",
                        CStr::from_ptr(dds_strretcode(-reader))
                            .to_str()
                            .unwrap_or("unrecoverable DDS retcode")
                    ))
                }
            }
            Some(period) => {
                // Use a periodic task that takes data to route from a Reader with KEEP_LAST 1
                qos.history = Some(History {
                    kind: HistoryKind::KEEP_LAST,
                    depth: 1,
                });
                let qos_native = qos.to_qos_native();
                let reader = dds_create_reader(dp, t, qos_native, std::ptr::null());
                task::spawn(async move {
                    // loop while reader's instance handle remain the same
                    // (if reader was deleted, its dds_entity_t value might have been
                    // reused by a new entity... don't trust it! Only trust instance handle)
                    let mut original_handle: dds_instance_handle_t = 0;
                    dds_get_instance_handle(reader, &mut original_handle);
                    let mut handle: dds_instance_handle_t = 0;
                    while dds_get_instance_handle(reader, &mut handle) == DDS_RETCODE_OK as i32 {
                        if handle != original_handle {
                            break;
                        }

                        tokio::time::sleep(period).await;
                        let mut zp: *mut ddsi_serdata = std::ptr::null_mut();
                        #[allow(clippy::uninit_assumed_init)]
                        let mut si = MaybeUninit::<[dds_sample_info_t; 1]>::uninit();
                        while dds_takecdr(
                            reader,
                            &mut zp,
                            1,
                            si.as_mut_ptr() as *mut dds_sample_info_t,
                            DDS_ANY_STATE,
                        ) > 0
                        {
                            let si = si.assume_init();
                            if si[0].valid_data {
                                let raw_sample = DDSRawSample::create(zp);
                                callback(&raw_sample);
                            }
                            ddsi_serdata_unref(zp);
                        }
                    }
                });
                Ok(reader)
            }
        }
    }
}
