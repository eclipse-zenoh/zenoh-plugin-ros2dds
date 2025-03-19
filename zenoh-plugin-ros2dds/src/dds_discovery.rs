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
use std::{ffi::CStr, fmt, mem::MaybeUninit, sync::Arc};

use cyclors::{qos::Qos, *};
use flume::Sender;
use serde::{Deserialize, Serialize};

use crate::{dds_types::TypeInfo, gid::Gid};

const MAX_SAMPLES: usize = 32;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DdsEntity {
    pub key: Gid,
    pub participant_key: Gid,
    pub topic_name: String,
    pub type_name: String,
    // We are storing the type_info but such information is not propagated for the time being
    #[serde(skip)]
    pub _type_info: Option<Arc<TypeInfo>>,
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

unsafe extern "C" fn on_data(dr: dds_entity_t, arg: *mut std::os::raw::c_void) {
    let btx = Box::from_raw(arg as *mut (DiscoveryType, Sender<DDSDiscoveryEvent>));
    let discovery_type = btx.0;
    let sender = &btx.1;
    let dp = dds_get_participant(dr);
    let mut dpih: dds_instance_handle_t = 0;
    let _ = dds_get_instance_handle(dp, &mut dpih);

    #[allow(clippy::uninit_assumed_init)]
    let mut si = MaybeUninit::<[dds_sample_info_t; MAX_SAMPLES]>::uninit();
    let mut samples: [*mut ::std::os::raw::c_void; MAX_SAMPLES] =
        [std::ptr::null_mut(); MAX_SAMPLES];
    samples[0] = std::ptr::null_mut();

    let n = dds_take(
        dr,
        samples.as_mut_ptr(),
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
                            tracing::warn!("Discovery of an invalid topic name: {}", e);
                            continue;
                        }
                    };
                    if topic_name.starts_with("DCPS") {
                        tracing::debug!(
                            "Ignoring discovery of {} ({} is a builtin topic)",
                            key,
                            topic_name
                        );
                        continue;
                    }

                    let type_name = match CStr::from_ptr((*sample).type_name).to_str() {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!("Discovery of an invalid topic type: {}", e);
                            continue;
                        }
                    };
                    let participant_key = (*sample).participant_key.v.into();
                    let keyless = (*sample).key.v[15] == 3 || (*sample).key.v[15] == 4;

                    tracing::debug!(
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
                                tracing::trace!(
                                    "Type information not available for type {}",
                                    type_name
                                );
                                None
                            }
                        },
                        _ => {
                            tracing::warn!(
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
                        _type_info: type_info,
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
                    tracing::debug!("Discovered DDS Participant {})", key,);

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
    dds_return_loan(dr, samples.as_mut_ptr(), MAX_SAMPLES as i32);
    let _ = Box::into_raw(btx);
}

fn send_discovery_event(sender: &Sender<DDSDiscoveryEvent>, event: DDSDiscoveryEvent) {
    if let Err(e) = sender.try_send(event) {
        tracing::error!(
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
