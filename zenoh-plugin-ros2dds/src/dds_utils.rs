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
    collections::{BTreeSet, HashSet},
    ffi::{CStr, CString},
    sync::Arc,
};

use cyclors::*;
use serde::{ser::SerializeMap, Serialize, Serializer};

use crate::{dds_types::TypeInfo, gid::Gid};

// A forwarding callback or graph publication must not prevent lifecycle progress.
pub(crate) const MAX_DDS_WRITE_BLOCKING_TIME: i64 = 100_000_000;

// Finite application write timeouts are preserved, so retirement can wait for
// them. Surface the ones long enough to look like a stalled bridge.
pub(crate) const SLOW_RETIREMENT_WARN_NS: i64 = 1_000_000_000;

pub(crate) static DDS_WRITE_FAILURES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Preserve legacy node names and add the participant identity, without storing
/// a second membership set. Flattened into each route's admin representation.
pub fn serialize_local_nodes<S: Serializer>(
    set: &HashSet<(Gid, String)>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    #[derive(Serialize)]
    struct Identity<'a> {
        participant: Gid,
        name: &'a str,
    }
    let names: BTreeSet<&str> = set.iter().map(|(_, name)| name.as_str()).collect();
    let sorted: BTreeSet<_> = set.iter().collect();
    let identities: Vec<_> = sorted
        .into_iter()
        .map(|(participant, name)| Identity {
            participant: *participant,
            name,
        })
        .collect();
    let mut map = serializer.serialize_map(Some(2))?;
    map.serialize_entry("local_nodes", &names)?;
    map.serialize_entry("local_node_identities", &identities)?;
    map.end()
}

pub const CDR_HEADER_LE: [u8; 4] = [0, 1, 0, 0];

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
            let mut topic: dds_entity_t = ret;
            if ret == (DDS_RETCODE_OK as i32) {
                topic = dds_create_topic(dp, descriptor, cton, std::ptr::null(), std::ptr::null());
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

pub fn dds_write(data_writer: dds_entity_t, data: Vec<u8>) -> Result<(), String> {
    let result = dds_write_inner(data_writer, data);
    if result.is_err() {
        DDS_WRITE_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    result
}

fn dds_write_inner(data_writer: dds_entity_t, data: Vec<u8>) -> Result<(), String> {
    unsafe {
        // Cyclone copies the serialized input into its serdata. Keep the Vec
        // owned here so every error path releases it, including size conversion.
        let size = ddsrt_iov_len_from_usize(data.len())?;
        let data_out = ddsrt_iovec_t {
            iov_base: data.as_ptr() as *mut std::ffi::c_void,
            iov_len: size,
        };

        let mut sertype_ptr: *const ddsi_sertype = std::ptr::null_mut();
        let ret = dds_get_entity_sertype(data_writer, &mut sertype_ptr);
        if ret < 0 {
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
            data.len(),
        );

        let ret = dds_writecdr(data_writer, fwdp);
        if ret < 0 {
            return Err(format!(
                "DDS write failed: {}",
                CStr::from_ptr(dds_strretcode(ret))
                    .to_str()
                    .unwrap_or("unrecoverable DDS retcode")
            ));
        }

        Ok(())
    }
}
