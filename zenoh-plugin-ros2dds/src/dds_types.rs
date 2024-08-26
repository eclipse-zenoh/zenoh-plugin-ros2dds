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
use std::fmt;
use std::slice;
use zenoh::buffers::ZBuf;
use zenoh::prelude::*;

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
    pub unsafe fn create(serdata: *const ddsi_serdata) -> Result<DDSRawSample, String> {
        let sdref: *mut ddsi_serdata;
        let mut data = ddsrt_iovec_t {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        };

        if (*serdata).loan.is_null() {
            let size = ddsi_serdata_size(serdata);
            sdref = ddsi_serdata_to_ser_ref(serdata, 0, size as usize, &mut data);
        } else {
            let loan = (*serdata).loan;
            let metadata = (*loan).metadata;

            // Based on the current Cyclone DDS implementation loan should only contain RAW sample data at this point
            if (*metadata).sample_state == dds_loaned_sample_state_DDS_LOANED_SAMPLE_STATE_RAW_DATA
            {
                // Before forwarding to Zenoh the data first needs to be serialized
                if (*(*serdata).ops).from_sample.is_some() {
                    // We have the type information necessary to serialize so use from_sample()
                    let serialized_serdata = ddsi_serdata_from_sample(
                        (*serdata).type_,
                        (*serdata).kind,
                        (*loan).sample_ptr,
                    );

                    let size = ddsi_serdata_size(serialized_serdata);
                    sdref =
                        ddsi_serdata_to_ser_ref(serialized_serdata, 0, size as usize, &mut data);
                    ddsi_serdata_unref(serialized_serdata);
                } else {
                    // Type information not available so unable to serialize sample using from_sample()
                    return Err(String::from(
                        "sample contains a loan for which incomplete type information is held",
                    ));
                }
            } else {
                return Err(String::from(
                    "sample contains a loan with an unexpected sample state",
                ));
            }
        }

        Ok(DDSRawSample { sdref, data })
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
        #[cfg(not(target_os = "windows"))]
        unsafe {
            &slice::from_raw_parts(self.data.iov_base as *const u8, self.data.iov_len)[4..]
        }
        #[cfg(target_os = "windows")]
        unsafe {
            &slice::from_raw_parts(self.data.iov_base as *const u8, self.data.iov_len as usize)[4..]
        }
    }

    pub fn hex_encode(&self) -> String {
        let mut encoded = String::new();
        let data_encoded = hex::encode(self.data_as_slice());
        encoded.push_str(data_encoded.as_str());
        encoded
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

impl From<&DDSRawSample> for ZBuf {
    fn from(buf: &DDSRawSample) -> Self {
        buf.data_as_slice().to_vec().into()
    }
}

impl From<&DDSRawSample> for Value {
    fn from(buf: &DDSRawSample) -> Self {
        ZBuf::from(buf).into()
    }
}
