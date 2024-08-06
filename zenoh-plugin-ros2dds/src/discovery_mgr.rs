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
    sync::{Arc, RwLock},
    time::Duration,
};

use cyclors::dds_entity_t;
use flume::{unbounded, Receiver, Sender};
use futures::{executor::block_on, select};
use tokio::task;
use zenoh::{
    internal::{zread, zwrite, TimedEvent, Timer},
    key_expr::keyexpr,
    query::Query,
};

use crate::{
    dds_discovery::*, discovered_entities::DiscoveredEntities, events::ROS2DiscoveryEvent,
    ros_discovery::*, ChannelEvent, ROS_DISCOVERY_INFO_POLL_INTERVAL_MS,
};

pub struct DiscoveryMgr {
    pub participant: dds_entity_t,
    pub ros_discovery_mgr: Arc<RosDiscoveryInfoMgr>,
    pub discovered_entities: Arc<RwLock<DiscoveredEntities>>,
}

impl DiscoveryMgr {
    pub fn create(
        participant: dds_entity_t,
        ros_discovery_mgr: Arc<RosDiscoveryInfoMgr>,
    ) -> DiscoveryMgr {
        DiscoveryMgr {
            participant,
            ros_discovery_mgr,
            discovered_entities: Arc::new(RwLock::new(Default::default())),
        }
    }

    pub async fn run(&mut self, evt_sender: Sender<ROS2DiscoveryEvent>) {
        // run DDS discovery
        let (dds_disco_snd, dds_disco_rcv): (
            Sender<DDSDiscoveryEvent>,
            Receiver<DDSDiscoveryEvent>,
        ) = unbounded();
        run_discovery(self.participant, dds_disco_snd);

        let ros_discovery_mgr = self.ros_discovery_mgr.clone();
        let discovered_entities = self.discovered_entities.clone();

        task::spawn(async move {
            // Timer for periodic read of "ros_discovery_info" topic
            let timer = Timer::default();
            let (tx, ros_disco_timer_rcv): (Sender<()>, Receiver<()>) = unbounded();
            let ros_disco_timer_event = TimedEvent::periodic(
                Duration::from_millis(ROS_DISCOVERY_INFO_POLL_INTERVAL_MS),
                ChannelEvent { tx },
            );
            timer.add_async(ros_disco_timer_event).await;

            loop {
                select!(
                    evt = dds_disco_rcv.recv_async() => {
                        match evt.unwrap() {
                            DDSDiscoveryEvent::DiscoveredParticipant {entity} => {
                                zwrite!(discovered_entities).add_participant(entity);
                            },
                            DDSDiscoveryEvent::UndiscoveredParticipant {key} => {
                                let evts = zwrite!(discovered_entities).remove_participant(&key);
                                for e in evts {
                                    if let Err(err) = evt_sender.try_send(e) {
                                        tracing::error!("Internal error: failed to send DDSDiscoveryEvent to main loop: {err}");
                                    }
                                }
                            },
                            DDSDiscoveryEvent::DiscoveredPublication{entity} => {
                                let e = zwrite!(discovered_entities).add_writer(entity);
                                if let Some(e) = e {
                                    if let Err(err) = evt_sender.try_send(e) {
                                        tracing::error!("Internal error: failed to send DDSDiscoveryEvent to main loop: {err}");
                                    }
                                }
                            },
                            DDSDiscoveryEvent::UndiscoveredPublication{key} => {
                                let e = zwrite!(discovered_entities).remove_writer(&key);
                                if let Some(e) = e {
                                    if let Err(err) = evt_sender.try_send(e) {
                                        tracing::error!("Internal error: failed to send DDSDiscoveryEvent to main loop: {err}");
                                    }
                                }
                            },
                            DDSDiscoveryEvent::DiscoveredSubscription {entity} => {
                                let e = zwrite!(discovered_entities).add_reader(entity);
                                if let Some(e) = e {
                                    if let Err(err) = evt_sender.try_send(e) {
                                        tracing::error!("Internal error: failed to send DDSDiscoveryEvent to main loop: {err}");
                                    }
                                }
                            },
                            DDSDiscoveryEvent::UndiscoveredSubscription {key} => {
                                let e = zwrite!(discovered_entities).remove_reader(&key);
                                if let Some(e) = e {
                                    if let Err(err) = evt_sender.try_send(e) {
                                        tracing::error!("Internal error: failed to send DDSDiscoveryEvent to main loop: {err}");
                                    }
                                }
                            },
                        }
                    }

                    _ = ros_disco_timer_rcv.recv_async() => {
                        let infos = ros_discovery_mgr.read();
                        for part_info in infos {
                            tracing::debug!("Received ros_discovery_info from {}", part_info);
                            let evts = zwrite!(discovered_entities).update_participant_info(part_info);
                            for e in evts {
                                if let Err(err) = evt_sender.try_send(e) {
                                    tracing::error!("Internal error: failed to send DDSDiscoveryEvent to main loop: {err}");
                                }
                            }
                        }
                    }
                )
            }
        });
    }

    pub fn treat_admin_query(&self, query: &Query, admin_keyexpr_prefix: &keyexpr) {
        // pass query to discovered_entities
        let discovered_entities = zread!(self.discovered_entities);
        // TODO: find a better solution than block_on()
        block_on(discovered_entities.treat_admin_query(query, admin_keyexpr_prefix))
    }
}
