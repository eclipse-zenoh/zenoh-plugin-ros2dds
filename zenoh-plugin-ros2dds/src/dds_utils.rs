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
use cyclors::*;
use std::ffi::CStr;
#[cfg(feature = "dds_shm")]
use zenoh::buffers::ZSlice;

use crate::vec_into_raw_parts;

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

pub fn dds_write(data_writer: dds_entity_t, data: Vec<u8>) -> Result<(), String> {
    unsafe {
        // As per the Vec documentation (see https://doc.rust-lang.org/std/vec/struct.Vec.html#method.into_raw_parts)
        // the only way to correctly releasing it is to create a vec using from_raw_parts
        // and then have its destructor do the cleanup.
        // Thus, while tempting to just pass the raw pointer to cyclone and then free it from C,
        // that is not necessarily safe or guaranteed to be leak free.
        // TODO replace when stable https://github.com/rust-lang/rust/issues/65816
        let (ptr, len, capacity) = vec_into_raw_parts(data);
        let size: ddsrt_iov_len_t = len
            .try_into()
            .map_err(|e| format!("DDS write failed: {e}"))?;

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
