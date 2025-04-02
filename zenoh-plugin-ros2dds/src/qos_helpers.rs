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
use cyclors::{qos::*, DDS_LENGTH_UNLIMITED};

pub fn get_history_or_default(qos: &Qos) -> History {
    match &qos.history {
        None => History::default(),
        Some(history) => history.clone(),
    }
}

pub fn get_durability_service_or_default(qos: &Qos) -> DurabilityService {
    match &qos.durability_service {
        None => DurabilityService::default(),
        Some(durability_service) => durability_service.clone(),
    }
}

pub fn is_reliable(qos: &Qos) -> bool {
    qos.reliability
        .as_ref()
        .is_some_and(|reliability| reliability.kind == ReliabilityKind::RELIABLE)
}

pub fn is_transient_local(qos: &Qos) -> bool {
    qos.durability
        .as_ref()
        .is_some_and(|durability| durability.kind == DurabilityKind::TRANSIENT_LOCAL)
}

// Copy and adapt Writer's QoS for creation of a matching Reader
pub fn adapt_writer_qos_for_reader(qos: &Qos) -> Qos {
    let mut reader_qos = qos.clone();

    // Unset any writer QoS that doesn't apply to data readers
    reader_qos.durability_service = None;
    reader_qos.ownership_strength = None;
    reader_qos.transport_priority = None;
    reader_qos.lifespan = None;
    reader_qos.writer_data_lifecycle = None;
    reader_qos.writer_batching = None;

    // Unset proprietary QoS which shouldn't apply
    reader_qos.properties = None;
    reader_qos.entity_name = None;
    reader_qos.ignore_local = Some(IgnoreLocal {
        kind: IgnoreLocalKind::PARTICIPANT,
    });

    // Set default Reliability QoS if not set for writer
    if reader_qos.reliability.is_none() {
        reader_qos.reliability = Some({
            Reliability {
                kind: ReliabilityKind::BEST_EFFORT,
                max_blocking_time: DDS_100MS_DURATION,
            }
        });
    }

    reader_qos
}

// Copy and adapt Reader's QoS for creation of a matching Writer
pub fn adapt_reader_qos_for_writer(qos: &Qos) -> Qos {
    let mut writer_qos = qos.clone();

    // Unset any reader QoS that doesn't apply to data writers
    writer_qos.time_based_filter = None;
    writer_qos.reader_data_lifecycle = None;
    writer_qos.properties = None;
    writer_qos.entity_name = None;

    // Don't match with readers with the same participant
    writer_qos.ignore_local = Some(IgnoreLocal {
        kind: IgnoreLocalKind::PARTICIPANT,
    });

    // if Reader is TRANSIENT_LOCAL, configure durability_service QoS with same history as the Reader.
    // This is because CycloneDDS is actually using durability_service.history for transient_local historical data.
    if is_transient_local(qos) {
        let history = qos
            .history
            .as_ref()
            .map_or(History::default(), |history| history.clone());

        writer_qos.durability_service = Some(DurabilityService {
            service_cleanup_delay: 60 * DDS_1S_DURATION,
            history_kind: history.kind,
            history_depth: history.depth,
            max_samples: DDS_LENGTH_UNLIMITED,
            max_instances: DDS_LENGTH_UNLIMITED,
            max_samples_per_instance: DDS_LENGTH_UNLIMITED,
        });
    }
    // Workaround for the DDS Writer to correctly match with a FastRTPS Reader
    writer_qos.reliability = match writer_qos.reliability {
        Some(mut reliability) => {
            reliability.max_blocking_time = reliability.max_blocking_time.saturating_add(1);
            Some(reliability)
        }
        _ => {
            let mut reliability = Reliability {
                kind: ReliabilityKind::RELIABLE,
                max_blocking_time: DDS_100MS_DURATION,
            };
            reliability.max_blocking_time = reliability.max_blocking_time.saturating_add(1);
            Some(reliability)
        }
    };

    writer_qos
}
