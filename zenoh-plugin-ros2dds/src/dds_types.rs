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

#[cfg(feature = "dds_shm")]
#[derive(Clone, Copy)]
struct IoxChunk {
    ptr: *mut std::ffi::c_void,
    header: *mut iceoryx_header_t,
}

#[cfg(feature = "dds_shm")]
impl IoxChunk {
    fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr as *const u8, (*self.header).data_size as usize) }
    }

    fn len(&self) -> usize {
        unsafe { (*self.header).data_size as usize }
    }
}

pub struct DDSRawSample {
    sdref: *mut ddsi_serdata,
    data: ddsrt_iovec_t,
    #[cfg(feature = "dds_shm")]
    iox_chunk: Option<IoxChunk>,
}

impl DDSRawSample {
    pub unsafe fn create(serdata: *const ddsi_serdata) -> DDSRawSample {
        let mut sdref: *mut ddsi_serdata = std::ptr::null_mut();
        let mut data = ddsrt_iovec_t {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        };

        #[cfg(feature = "dds_shm")]
        let iox_chunk: Option<IoxChunk> = match ((*serdata).iox_chunk).is_null() {
            false => {
                let iox_chunk_ptr = (*serdata).iox_chunk;
                let header = iceoryx_header_from_chunk(iox_chunk_ptr);

                // If the Iceoryx chunk contains raw sample data this needs to be serialized before forwading to Zenoh
                if (*header).shm_data_state == iox_shm_data_state_t_IOX_CHUNK_CONTAINS_RAW_DATA {
                    let serialized_serdata = ddsi_serdata_from_sample(
                        (*serdata).type_,
                        (*serdata).kind,
                        (*serdata).iox_chunk,
                    );

                    let size = ddsi_serdata_size(serialized_serdata);
                    sdref =
                        ddsi_serdata_to_ser_ref(serialized_serdata, 0, size as usize, &mut data);
                    ddsi_serdata_unref(serialized_serdata);

                    // IoxChunk not needed where raw data has been serialized
                    None
                } else {
                    Some(IoxChunk {
                        ptr: iox_chunk_ptr,
                        header,
                    })
                }
            }
            true => None,
        };

        // At this point sdref will be null if:
        //
        // * Iceoryx was not enabled/used - in this case data will contain the CDR header and payload
        // * Iceoryx chunk contained serialized data - in this case data will contain the CDR header
        if sdref.is_null() {
            let size = ddsi_serdata_size(serdata);
            sdref = ddsi_serdata_to_ser_ref(serdata, 0, size as usize, &mut data);
        }

        #[cfg(feature = "dds_shm")]
        return DDSRawSample {
            sdref,
            data,
            iox_chunk,
        };
        #[cfg(not(feature = "dds_shm"))]
        return DDSRawSample { sdref, data };
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
            #[cfg(feature = "dds_shm")]
            {
                if let Some(iox_chunk) = self.iox_chunk.as_ref() {
                    return iox_chunk.as_slice();
                }
            }
            &slice::from_raw_parts(
                self.data.iov_base as *const u8,
                ddsrt_iov_len_to_usize(self.data.iov_len).unwrap(),
            )[4..]
        }
    }

    pub fn hex_encode(&self) -> String {
        let mut encoded = String::new();
        let data_encoded = hex::encode(self.data_as_slice());
        encoded.push_str(data_encoded.as_str());

        #[cfg(feature = "dds_shm")]
        {
            if let Some(iox_chunk) = self.iox_chunk.as_ref() {
                let iox_encoded = hex::encode(iox_chunk.as_slice());
                encoded.push_str(iox_encoded.as_str());
            }
        }

        encoded
    }

    pub fn len(&self) -> usize {
        #[cfg(feature = "dds_shm")]
        {
            TryInto::<usize>::try_into(self.data.iov_len).unwrap()
                + self.iox_chunk.as_ref().map(IoxChunk::len).unwrap_or(0)
        }

        #[cfg(not(feature = "dds_shm"))]
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
        #[cfg(feature = "dds_shm")]
        {
            // Where data was received via Iceoryx write both the header (contained in buf.data) and
            // payload (contained in buf.iox_chunk) to the formatter.
            if let Some(iox_chunk) = self.iox_chunk {
                return write!(
                    f,
                    "[{:02x?}, {:02x?}]",
                    self.data_as_slice(),
                    iox_chunk.as_slice()
                );
            }
        }
        write!(f, "{:02x?}", self.data_as_slice())
    }
}

impl From<&DDSRawSample> for ZBytes {
    fn from(buf: &DDSRawSample) -> Self {
        #[cfg(feature = "dds_shm")]
        {
            // Where data was received via Iceoryx return both the header (contained in buf.data) and
            // payload (contained in buf.iox_chunk) in a buffer.
            if let Some(iox_chunk) = buf.iox_chunk {
                let mut buf_and_iox_chunk = buf.data_as_slice().to_vec();
                buf_and_iox_chunk.append(&mut iox_chunk.as_slice().to_vec());
                let z_bytes = ZBytes::from(buf_and_iox_chunk);
                return z_bytes;
            }
        }
        buf.data_as_slice().into()
    }
}
