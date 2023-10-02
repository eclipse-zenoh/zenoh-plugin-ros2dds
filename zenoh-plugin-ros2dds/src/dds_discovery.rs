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
use async_std::task;
use cyclors::qos::{History, HistoryKind, Qos};
use cyclors::*;
use flume::Sender;
use serde::{Deserialize, Serialize, Serializer};
use std::ffi::{CStr, CString};
use std::fmt;
use std::mem::MaybeUninit;
use std::os::raw;
use std::slice;
use std::sync::Arc;
use std::time::Duration;
use zenoh::buffers::ZBuf;
#[cfg(feature = "dds_shm")]
use zenoh::buffers::ZSlice;
use zenoh::prelude::*;
use zenoh::publication::CongestionControl;
use zenoh::Session;
use zenoh_core::SyncResolve;

use crate::gid::Gid;

const MAX_SAMPLES: usize = 32;

#[derive(Debug)]
pub struct TypeInfo {
    ptr: *mut dds_typeinfo_t,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DdsEntity {
    pub key: Gid,
    pub participant_key: Gid,
    pub topic_name: String,
    pub type_name: String,
    #[serde(skip)]
    pub type_info: Option<Arc<TypeInfo>>,
    pub keyless: bool,
    pub qos: Qos,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DdsParticipant {
    pub key: Gid,
    pub qos: Qos,
}

#[derive(Debug)]
pub enum DDSDiscoveryEvent {
    DiscoveredPublication { entity: DdsEntity },
    UndiscoveredPublication { key: Gid },
    DiscoveredSubscription { entity: DdsEntity },
    UndiscoveredSubscription { key: Gid },
    DiscoveredParticipant { entity: DdsParticipant },
    UndiscoveredParticipant { key: Gid },
}

#[derive(Debug, Clone, Copy)]
pub enum DiscoveryType {
    Participant,
    Publication,
    Subscription,
}

impl fmt::Display for DiscoveryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiscoveryType::Participant => write!(f, "participant"),
            DiscoveryType::Publication => write!(f, "publication"),
            DiscoveryType::Subscription => write!(f, "subscription"),
        }
    }
}

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
                self.data.iov_len.try_into().unwrap(),
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
                self.data.iov_len.try_into().unwrap(),
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
        self.data.iov_len.try_into().unwrap()
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

impl From<&DDSRawSample> for ZBuf {
    fn from(buf: &DDSRawSample) -> Self {
        #[cfg(feature = "dds_shm")]
        {
            // Where data was received via Iceoryx return both the header (contained in buf.data) and
            // payload (contained in buf.iox_chunk) in a buffer.
            if let Some(iox_chunk) = buf.iox_chunk {
                let mut zbuf = ZBuf::default();
                zbuf.push_zslice(ZSlice::from(buf.data_as_slice().to_vec()));
                zbuf.push_zslice(ZSlice::from(iox_chunk.as_slice().to_vec()));
                return zbuf;
            }
        }
        buf.data_as_slice().to_vec().into()
    }
}

impl From<&DDSRawSample> for Value {
    fn from(buf: &DDSRawSample) -> Self {
        ZBuf::from(buf).into()
    }
}

unsafe extern "C" fn on_data(dr: dds_entity_t, arg: *mut std::os::raw::c_void) {
    let btx = Box::from_raw(arg as *mut (DiscoveryType, Sender<DDSDiscoveryEvent>));
    let discovery_type = btx.0;
    let sender = &btx.1;
    let dp = dds_get_participant(dr);
    let mut dpih: dds_instance_handle_t = 0;
    let _ = dds_get_instance_handle(dp, &mut dpih);

    #[allow(clippy::uninit_assumed_init)]
    let mut si = MaybeUninit::<[dds_sample_info_t; MAX_SAMPLES as usize]>::uninit();
    let mut samples: [*mut ::std::os::raw::c_void; MAX_SAMPLES as usize] =
        [std::ptr::null_mut(); MAX_SAMPLES as usize];
    samples[0] = std::ptr::null_mut();

    let n = dds_take(
        dr,
        samples.as_mut_ptr() as *mut *mut raw::c_void,
        si.as_mut_ptr() as *mut dds_sample_info_t,
        MAX_SAMPLES,
        MAX_SAMPLES as u32,
    );
    let si = si.assume_init();

    for i in 0..n {
        match discovery_type {
            DiscoveryType::Publication | DiscoveryType::Subscription => {
                let sample = samples[i as usize] as *mut dds_builtintopic_endpoint_t;
                if (*sample).participant_instance_handle == dpih {
                    // Ignore discovery of entities created by our own participant
                    continue;
                }
                let is_alive = si[i as usize].instance_state == dds_instance_state_DDS_IST_ALIVE;
                let key: Gid = (*sample).key.v.into();

                if is_alive {
                    let topic_name = match CStr::from_ptr((*sample).topic_name).to_str() {
                        Ok(s) => s,
                        Err(e) => {
                            log::warn!("Discovery of an invalid topic name: {}", e);
                            continue;
                        }
                    };
                    if topic_name.starts_with("DCPS") {
                        log::debug!(
                            "Ignoring discovery of {} ({} is a builtin topic)",
                            key,
                            topic_name
                        );
                        continue;
                    }

                    let type_name = match CStr::from_ptr((*sample).type_name).to_str() {
                        Ok(s) => s,
                        Err(e) => {
                            log::warn!("Discovery of an invalid topic type: {}", e);
                            continue;
                        }
                    };
                    let participant_key = (*sample).participant_key.v.into();
                    let keyless = (*sample).key.v[15] == 3 || (*sample).key.v[15] == 4;

                    log::debug!(
                        "Discovered DDS {} {} from Participant {} on {} with type {} (keyless: {})",
                        discovery_type,
                        key,
                        participant_key,
                        topic_name,
                        type_name,
                        keyless
                    );

                    let mut type_info: *const dds_typeinfo_t = std::ptr::null();
                    let ret = dds_builtintopic_get_endpoint_type_info(sample, &mut type_info);

                    let type_info = match ret {
                        0 => match type_info.is_null() {
                            false => Some(Arc::new(TypeInfo::new(type_info))),
                            true => {
                                log::trace!(
                                    "Type information not available for type {}",
                                    type_name
                                );
                                None
                            }
                        },
                        _ => {
                            log::warn!(
                                "Failed to lookup type information({})",
                                CStr::from_ptr(dds_strretcode(ret))
                                    .to_str()
                                    .unwrap_or("unrecoverable DDS retcode")
                            );
                            None
                        }
                    };

                    // send a DDSDiscoveryEvent
                    let entity = DdsEntity {
                        key,
                        participant_key,
                        topic_name: String::from(topic_name),
                        type_name: String::from(type_name),
                        keyless,
                        type_info,
                        qos: Qos::from_qos_native((*sample).qos),
                    };

                    if let DiscoveryType::Publication = discovery_type {
                        send_discovery_event(
                            sender,
                            DDSDiscoveryEvent::DiscoveredPublication { entity },
                        );
                    } else {
                        send_discovery_event(
                            sender,
                            DDSDiscoveryEvent::DiscoveredSubscription { entity },
                        );
                    }
                } else if let DiscoveryType::Publication = discovery_type {
                    send_discovery_event(
                        sender,
                        DDSDiscoveryEvent::UndiscoveredPublication { key },
                    );
                } else {
                    send_discovery_event(
                        sender,
                        DDSDiscoveryEvent::UndiscoveredSubscription { key },
                    );
                }
            }
            DiscoveryType::Participant => {
                let sample = samples[i as usize] as *mut dds_builtintopic_participant_t;
                let is_alive = si[i as usize].instance_state == dds_instance_state_DDS_IST_ALIVE;
                let key: Gid = (*sample).key.v.into();

                let mut guid = dds_builtintopic_guid { v: [0; 16] };
                let _ = dds_get_guid(dp, &mut guid);
                let guid = guid.v.into();

                if key == guid {
                    // Ignore discovery of entities created by our own participant
                    continue;
                }

                if is_alive {
                    log::debug!("Discovered DDS Participant {})", key,);

                    // Send a DDSDiscoveryEvent
                    let entity = DdsParticipant {
                        key,
                        qos: Qos::from_qos_native((*sample).qos),
                    };

                    send_discovery_event(
                        sender,
                        DDSDiscoveryEvent::DiscoveredParticipant { entity },
                    );
                } else {
                    send_discovery_event(
                        sender,
                        DDSDiscoveryEvent::UndiscoveredParticipant { key },
                    );
                }
            }
        }
    }
    dds_return_loan(
        dr,
        samples.as_mut_ptr() as *mut *mut raw::c_void,
        MAX_SAMPLES as i32,
    );
    Box::into_raw(btx);
}

fn send_discovery_event(sender: &Sender<DDSDiscoveryEvent>, event: DDSDiscoveryEvent) {
    if let Err(e) = sender.try_send(event) {
        log::error!(
            "INTERNAL ERROR sending DDSDiscoveryEvent to internal channel: {:?}",
            e
        );
    }
}

pub fn run_discovery(dp: dds_entity_t, tx: Sender<DDSDiscoveryEvent>) {
    unsafe {
        let ptx = Box::new((DiscoveryType::Publication, tx.clone()));
        let stx = Box::new((DiscoveryType::Subscription, tx.clone()));
        let dptx = Box::new((DiscoveryType::Participant, tx));
        let sub_listener = dds_create_listener(Box::into_raw(ptx) as *mut std::os::raw::c_void);
        dds_lset_data_available(sub_listener, Some(on_data));

        let _pr = dds_create_reader(
            dp,
            DDS_BUILTIN_TOPIC_DCPSPUBLICATION,
            std::ptr::null(),
            sub_listener,
        );

        let sub_listener = dds_create_listener(Box::into_raw(stx) as *mut std::os::raw::c_void);
        dds_lset_data_available(sub_listener, Some(on_data));
        let _sr = dds_create_reader(
            dp,
            DDS_BUILTIN_TOPIC_DCPSSUBSCRIPTION,
            std::ptr::null(),
            sub_listener,
        );

        let sub_listener = dds_create_listener(Box::into_raw(dptx) as *mut std::os::raw::c_void);
        dds_lset_data_available(sub_listener, Some(on_data));
        let _dpr = dds_create_reader(
            dp,
            DDS_BUILTIN_TOPIC_DCPSPARTICIPANT,
            std::ptr::null(),
            sub_listener,
        );
    }
}

unsafe extern "C" fn data_forwarder_listener(dr: dds_entity_t, arg: *mut std::os::raw::c_void) {
    let pa = arg as *mut (String, KeyExpr, Arc<Session>, CongestionControl);
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

            if *crate::LOG_PAYLOAD {
                log::trace!(
                    "Route Publisher (DDS:{} -> Zenoh:{}) - routing data - payload: {:02x?}",
                    &(*pa).0,
                    &(*pa).1,
                    raw_sample
                );
            } else {
                log::trace!(
                    "Route Publisher (DDS:{} -> Zenoh:{}) - routing data - {} bytes",
                    &(*pa).0,
                    &(*pa).1,
                    raw_sample.len()
                );
            }
            let _ = (*pa)
                .2
                .put(&(*pa).1, &raw_sample)
                .congestion_control((*pa).3)
                .res_sync();
        }
        ddsi_serdata_unref(zp);
    }
}

#[allow(clippy::too_many_arguments)]
pub fn create_forwarding_dds_reader(
    dp: dds_entity_t,
    topic_name: String,
    type_name: String,
    type_info: &Option<Arc<TypeInfo>>,
    keyless: bool,
    mut qos: Qos,
    z_key: KeyExpr,
    z: Arc<Session>,
    read_period: Option<Duration>,
    congestion_ctrl: CongestionControl,
) -> Result<dds_entity_t, String> {
    unsafe {
        let t = create_topic(dp, &topic_name, &type_name, type_info, keyless);

        match read_period {
            None => {
                // Use a Listener to route data as soon as it arrives
                let arg = Box::new((topic_name, z_key, z, congestion_ctrl));
                let sub_listener =
                    dds_create_listener(Box::into_raw(arg) as *mut std::os::raw::c_void);
                dds_lset_data_available(sub_listener, Some(data_forwarder_listener));
                let qos_native = qos.to_qos_native();
                let reader = dds_create_reader(dp, t, qos_native, sub_listener);
                Qos::delete_qos_native(qos_native);
                if reader >= 0 {
                    let res = dds_reader_wait_for_historical_data(reader, qos::DDS_100MS_DURATION);
                    if res < 0 {
                        log::error!(
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
                let z_key = z_key.into_owned();
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

                        async_std::task::sleep(period).await;
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
                                log::trace!(
                                    "Route (periodic) data to zenoh resource with rid={}",
                                    z_key
                                );

                                let raw_sample = DDSRawSample::create(zp);

                                let _ = z
                                    .put(&z_key, &raw_sample)
                                    .congestion_control(congestion_ctrl)
                                    .res_sync();
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

unsafe fn create_topic(
    dp: dds_entity_t,
    topic_name: &str,
    type_name: &str,
    type_info: &Option<Arc<TypeInfo>>,
    keyless: bool,
) -> dds_entity_t {
    let cton = CString::new(topic_name.to_owned()).unwrap().into_raw();
    let ctyn = CString::new(type_name.to_owned()).unwrap().into_raw();

    match type_info {
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
    }
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

unsafe extern "C" fn listener_to_callback<F>(dr: dds_entity_t, arg: *mut std::os::raw::c_void)
where
    F: Fn(&DDSRawSample) -> (),
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
    F: Fn(&DDSRawSample) -> () + std::marker::Send + 'static,
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
                        log::error!(
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

                        async_std::task::sleep(period).await;
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
