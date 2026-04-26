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

use std::{fmt, slice};

use cyclors::*;
use zenoh::bytes::ZBytes;

use crate::dds_utils::ddsrt_iov_len_to_usize;

#[derive(Debug)]
pub struct TypeInfo {
    pub(crate) ptr: *mut dds_typeinfo_t,
}

impl TypeInfo {
    pub unsafe fn new(ptr: *const dds_typeinfo_t) -> TypeInfo {
        let ptr = ddsi_typeinfo_dup(ptr);
        TypeInfo { ptr }
    }
}

impl Drop for TypeInfo {
    fn drop(&mut self) {
        unsafe {
            ddsi_typeinfo_free(self.ptr);
        }
    }
}

unsafe impl Send for TypeInfo {}
unsafe impl Sync for TypeInfo {}

pub struct DDSRawSample {
    sdref: *mut ddsi_serdata,
    data: ddsrt_iovec_t,
}

impl DDSRawSample {
    pub unsafe fn create(serdata: *const ddsi_serdata) -> DDSRawSample {
        let mut data = ddsrt_iovec_t {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        };

        // With cyclors 0.3.x / CycloneDDS PSMX, shared memory transport is handled
        // transparently by CycloneDDS. Data is always available in serialized form
        // through ddsi_serdata_to_ser_ref, regardless of the underlying transport.
        let size = ddsi_serdata_size(serdata);
        let sdref = ddsi_serdata_to_ser_ref(serdata, 0, size as usize, &mut data);

        DDSRawSample { sdref, data }
    }

    fn data_as_slice(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(
                self.data.iov_base as *const u8,
                ddsrt_iov_len_to_usize(self.data.iov_len).unwrap(),
            )
        }
    }

    pub fn payload_as_slice(&self) -> &[u8] {
        unsafe {
            &slice::from_raw_parts(
                self.data.iov_base as *const u8,
                ddsrt_iov_len_to_usize(self.data.iov_len).unwrap(),
            )[4..]
        }
    }

    pub fn hex_encode(&self) -> String {
        hex::encode(self.data_as_slice())
    }

    pub fn len(&self) -> usize {
        ddsrt_iov_len_to_usize(self.data.iov_len).unwrap()
    }
}

impl Drop for DDSRawSample {
    fn drop(&mut self) {
        unsafe {
            ddsi_serdata_to_ser_unref(self.sdref, &self.data);
        }
    }
}

impl fmt::Debug for DDSRawSample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:02x?}", self.data_as_slice())
    }
}

impl From<&DDSRawSample> for ZBytes {
    fn from(buf: &DDSRawSample) -> Self {
        buf.data_as_slice().into()
    }
}
