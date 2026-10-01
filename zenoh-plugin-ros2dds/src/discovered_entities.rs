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
    collections::HashMap,
    fmt::{self, Debug},
};

use zenoh::{
    bytes::{Encoding, ZBytes},
    key_expr::{
        format::{kedefine, keformat},
        keyexpr, OwnedKeyExpr,
    },
    query::Query,
};

use crate::{
    dds_discovery::{DDSDiscoveryEvent, DdsEntity, DdsParticipant},
    events::ROS2DiscoveryEvent,
    gid::Gid,
    node_info::*,
    ros_discovery::ParticipantEntitiesInfo,
};

kedefine!(
    pub(crate) ke_admin_participant: "dds/${pgid:*}",
    pub(crate) ke_admin_writer: "dds/${pgid:*}/writer/${wgid:*}/${topic:**}",
    pub(crate) ke_admin_reader: "dds/${pgid:*}/reader/${wgid:*}/${topic:**}",
    pub(crate) ke_admin_node: "node/${node_id:**}",
);

#[derive(Default)]
pub struct DiscoveredEntities {
    participants: HashMap<Gid, DdsParticipant>,
    writers: HashMap<Gid, DdsEntity>,
    readers: HashMap<Gid, DdsEntity>,
    ros_participant_info: HashMap<Gid, ParticipantEntitiesInfo>,
    nodes_info: HashMap<Gid, HashMap<String, NodeInfo>>,
    admin_space: HashMap<OwnedKeyExpr, EntityRef>,
}

impl Debug for DiscoveredEntities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "participants: {:?}",
            self.participants.keys().collect::<Vec<&Gid>>()
        )?;
        writeln!(
            f,
            "writers: {:?}",
            self.writers.keys().collect::<Vec<&Gid>>()
        )?;
        writeln!(
            f,
            "readers: {:?}",
            self.readers.keys().collect::<Vec<&Gid>>()
        )?;
        writeln!(f, "ros_participant_info: {:?}", self.ros_participant_info)?;
        writeln!(f, "nodes_info: {:?}", self.nodes_info)?;
        writeln!(
            f,
            "admin_space: {:?}",
            self.admin_space.keys().collect::<Vec<&OwnedKeyExpr>>()
        )
    }
}

#[derive(Debug)]
enum EntityRef {
    Participant(Gid),
    Writer(Gid),
    Reader(Gid),
    Node(Gid, String),
}

impl DiscoveredEntities {
    pub(crate) fn counts(&self) -> serde_json::Value {
        serde_json::json!({
            "participants": self.participants.len(),
            "ros_graphs": self.ros_participant_info.len(),
            "readers": self.readers.len(), "writers": self.writers.len(),
            "nodes": self.nodes_info.values().map(|nodes| nodes.len()).sum::<usize>(),
            "pending_endpoints": self.nodes_info.values().flat_map(|nodes| nodes.values())
                .map(|node| node.undiscovered_reader.len() + node.undiscovered_writer.len()).sum::<usize>(),
        })
    }

    #[inline]
    pub fn add_participant(&mut self, participant: DdsParticipant) -> Vec<ROS2DiscoveryEvent> {
        let gid = participant.key;
        self.admin_space.insert(
            keformat!(ke_admin_participant::formatter(), pgid = gid).unwrap(),
            EntityRef::Participant(gid),
        );
        self.participants.insert(gid, participant);
        self.reconcile_participant(gid)
    }

    #[inline]
    pub fn remove_participant(&mut self, gid: &Gid) -> Vec<ROS2DiscoveryEvent> {
        self.participants.remove(gid);
        self.ros_participant_info.remove(gid);
        self.writers
            .retain(|_, entity| entity.participant_key != *gid);
        self.readers
            .retain(|_, entity| entity.participant_key != *gid);
        self.admin_space.retain(|_, entity| match entity {
            EntityRef::Participant(p) | EntityRef::Node(p, _) => p != gid,
            EntityRef::Writer(w) => self.writers.contains_key(w),
            EntityRef::Reader(r) => self.readers.contains_key(r),
        });
        let mut events = Vec::new();
        if let Some(nodes) = self.nodes_info.remove(gid) {
            for (name, mut node) in nodes {
                tracing::info!("Undiscovered ROS Node {name}");
                events.extend(node.remove_all_entities());
            }
        }
        events
    }

    #[inline]
    fn add_writer(&mut self, writer: DdsEntity) -> Vec<ROS2DiscoveryEvent> {
        self.admin_space.insert(
            keformat!(
                ke_admin_writer::formatter(),
                pgid = writer.participant_key,
                wgid = writer.key,
                topic = &writer.topic_name
            )
            .unwrap(),
            EntityRef::Writer(writer.key),
        );
        let mut events = Vec::new();
        if let Some(nodes) = self.nodes_info.get_mut(&writer.participant_key) {
            for node in nodes.values_mut() {
                if let Some(index) = node
                    .undiscovered_writer
                    .iter()
                    .position(|gid| *gid == writer.key)
                {
                    node.undiscovered_writer.remove(index);
                    events.extend(node.update_with_writer(&writer));
                }
            }
        }
        self.writers.insert(writer.key, writer);
        events
    }

    #[inline]
    pub fn get_writer(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.writers.get(gid)
    }

    #[inline]
    fn remove_writer(&mut self, gid: &Gid) -> Vec<ROS2DiscoveryEvent> {
        let Some(writer) = self.writers.remove(gid) else {
            return Vec::new();
        };
        self.admin_space.remove(
            &keformat!(
                ke_admin_writer::formatter(),
                pgid = writer.participant_key,
                wgid = writer.key,
                topic = &writer.topic_name
            )
            .unwrap(),
        );
        let mut events = Vec::new();
        if let (Some(graph), Some(nodes)) = (
            self.ros_participant_info.get(&writer.participant_key),
            self.nodes_info.get_mut(&writer.participant_key),
        ) {
            // The endpoint is already out of the global map, so this holds
            // only survivors on the same topic within the same participant.
            let replacements: Vec<_> = self
                .writers
                .values()
                .filter(|candidate| {
                    candidate.participant_key == writer.participant_key
                        && candidate.topic_name == writer.topic_name
                })
                .collect();
            for (name, ros_node) in &graph.node_entities_info_seq {
                let membership = &ros_node.writer_gid_seq;
                if membership.contains(gid) {
                    let Some(node) = nodes.get_mut(name) else {
                        continue;
                    };
                    // Swap a tracked endpoint for its best survivor instead of
                    // withdrawing the shared interface; an untracked endpoint
                    // must not rewrite a live interface it never backed.
                    let (tracked, withdrawal) = node.remove_writer(gid);
                    if tracked {
                        match replacements
                            .iter()
                            .filter(|candidate| membership.contains(&candidate.key))
                            .max_by_key(|candidate| {
                                (candidate.type_name == writer.type_name, candidate.key)
                            }) {
                            Some(replacement) => push_replacement_events(
                                &mut events,
                                withdrawal,
                                node.update_with_writer(replacement),
                            ),
                            None => events.extend(withdrawal),
                        }
                    }
                    node.undiscovered_writer.push(*gid);
                }
            }
        }
        events
    }

    #[inline]
    fn add_reader(&mut self, reader: DdsEntity) -> Vec<ROS2DiscoveryEvent> {
        self.admin_space.insert(
            keformat!(
                ke_admin_reader::formatter(),
                pgid = reader.participant_key,
                wgid = reader.key,
                topic = &reader.topic_name
            )
            .unwrap(),
            EntityRef::Reader(reader.key),
        );
        let mut events = Vec::new();
        if let Some(nodes) = self.nodes_info.get_mut(&reader.participant_key) {
            for node in nodes.values_mut() {
                if let Some(index) = node
                    .undiscovered_reader
                    .iter()
                    .position(|gid| *gid == reader.key)
                {
                    node.undiscovered_reader.remove(index);
                    events.extend(node.update_with_reader(&reader));
                }
            }
        }
        self.readers.insert(reader.key, reader);
        events
    }

    #[inline]
    pub fn get_reader(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.readers.get(gid)
    }

    #[inline]
    fn remove_reader(&mut self, gid: &Gid) -> Vec<ROS2DiscoveryEvent> {
        let Some(reader) = self.readers.remove(gid) else {
            return Vec::new();
        };
        self.admin_space.remove(
            &keformat!(
                ke_admin_reader::formatter(),
                pgid = reader.participant_key,
                wgid = reader.key,
                topic = &reader.topic_name
            )
            .unwrap(),
        );
        let mut events = Vec::new();
        if let (Some(graph), Some(nodes)) = (
            self.ros_participant_info.get(&reader.participant_key),
            self.nodes_info.get_mut(&reader.participant_key),
        ) {
            // The endpoint is already out of the global map, so this holds
            // only survivors on the same topic within the same participant.
            let replacements: Vec<_> = self
                .readers
                .values()
                .filter(|candidate| {
                    candidate.participant_key == reader.participant_key
                        && candidate.topic_name == reader.topic_name
                })
                .collect();
            for (name, ros_node) in &graph.node_entities_info_seq {
                let membership = &ros_node.reader_gid_seq;
                if membership.contains(gid) {
                    let Some(node) = nodes.get_mut(name) else {
                        continue;
                    };
                    // Swap a tracked endpoint for its best survivor instead of
                    // withdrawing the shared interface; an untracked endpoint
                    // must not rewrite a live interface it never backed.
                    let (tracked, withdrawal) = node.remove_reader(gid);
                    if tracked {
                        match replacements
                            .iter()
                            .filter(|candidate| membership.contains(&candidate.key))
                            .max_by_key(|candidate| {
                                (candidate.type_name == reader.type_name, candidate.key)
                            }) {
                            Some(replacement) => push_replacement_events(
                                &mut events,
                                withdrawal,
                                node.update_with_reader(replacement),
                            ),
                            None => events.extend(withdrawal),
                        }
                    }
                    node.undiscovered_reader.push(*gid);
                }
            }
        }
        events
    }

    /// Update only the affected interface, as the eclipse-zenoh upstream
    /// does. ROS graph snapshots reconcile membership; DDS events must not
    /// reconstruct unrelated routes, even when the events arrive one at a
    /// time. A compatible survivor preserves the interface; a type change
    /// withdraws the old interface before announcing its replacement.
    pub fn apply_dds_event(&mut self, event: DDSDiscoveryEvent) -> Vec<ROS2DiscoveryEvent> {
        match event {
            DDSDiscoveryEvent::DiscoveredPublication { entity } => self.add_writer(entity),
            DDSDiscoveryEvent::UndiscoveredPublication { key } => self.remove_writer(&key),
            DDSDiscoveryEvent::DiscoveredSubscription { entity } => self.add_reader(entity),
            DDSDiscoveryEvent::UndiscoveredSubscription { key } => self.remove_reader(&key),
            DDSDiscoveryEvent::DiscoveredParticipant { entity } => self.add_participant(entity),
            DDSDiscoveryEvent::UndiscoveredParticipant { key } => self.remove_participant(&key),
        }
    }

    pub fn update_participant_info(
        &mut self,
        info: ParticipantEntitiesInfo,
    ) -> Vec<ROS2DiscoveryEvent> {
        let participant = info.gid;
        self.ros_participant_info.insert(participant, info);
        self.reconcile_participant(participant)
    }

    /// ROS graph membership and DDS endpoint metadata can arrive in either order.
    /// Derive this participant's nodes from their latest intersection, then emit
    /// the difference. This reconcile path never carries endpoints absent from
    /// the current ROS snapshot; incremental endpoint events are handled by
    /// `remove_reader`/`remove_writer`, which withdraw an interface only when
    /// its last tracked endpoint is gone. Within one returned batch, same-type
    /// discoveries precede withdrawals across all nodes, so transferring an
    /// interface preserves its route. Type changes withdraw the old route first.
    fn reconcile_participant(&mut self, participant: Gid) -> Vec<ROS2DiscoveryEvent> {
        if !self.participants.contains_key(&participant) {
            return Vec::new();
        }
        let Some(info) = self.ros_participant_info.get(&participant) else {
            return Vec::new();
        };
        let mut previous = self.nodes_info.remove(&participant).unwrap_or_default();
        let mut current = HashMap::new();
        let mut events = Vec::new();
        for (name, ros_node) in &info.node_entities_info_seq {
            let mut node = match NodeInfo::create(
                ros_node.node_namespace.clone(),
                ros_node.node_name.clone(),
                participant,
            ) {
                Ok(node) => node,
                Err(error) => {
                    tracing::warn!("ROS Node has incompatible name: {error}");
                    continue;
                }
            };
            // Stable ordering also makes overlapping same-name endpoints
            // deterministic. The route represents presence, not a chosen server.
            let mut readers: Vec<_> = ros_node.reader_gid_seq.iter().collect();
            readers.sort_unstable();
            for gid in readers {
                if let Some(entity) = self
                    .readers
                    .get(gid)
                    .filter(|e| e.participant_key == participant)
                {
                    node.update_with_reader(entity);
                } else {
                    node.undiscovered_reader.push(*gid);
                }
            }
            let mut writers: Vec<_> = ros_node.writer_gid_seq.iter().collect();
            writers.sort_unstable();
            for gid in writers {
                if let Some(entity) = self
                    .writers
                    .get(gid)
                    .filter(|e| e.participant_key == participant)
                {
                    node.update_with_writer(entity);
                } else {
                    node.undiscovered_writer.push(*gid);
                }
            }
            let old = previous.remove(name);
            let pending_changed = match &old {
                Some(previous) => {
                    previous.undiscovered_reader != node.undiscovered_reader
                        || previous.undiscovered_writer != node.undiscovered_writer
                }
                None => {
                    !node.undiscovered_reader.is_empty() || !node.undiscovered_writer.is_empty()
                }
            };
            if pending_changed {
                tracing::debug!(%participant, node = %name,
                    readers = ?node.undiscovered_reader, writers = ?node.undiscovered_writer,
                    "ROS graph endpoints awaiting DDS discovery changed");
            }
            if old.is_none() {
                tracing::info!("Discovered ROS Node {name}");
            }
            events.extend(node.delta(old.as_ref()));
            self.admin_space.insert(
                keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr()).unwrap(),
                EntityRef::Node(participant, name.clone()),
            );
            current.insert(name.clone(), node);
        }
        for (name, mut node) in previous {
            tracing::info!("Undiscovered ROS Node {name}");
            self.admin_space.remove(
                &keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr()).unwrap(),
            );
            events.extend(node.remove_all_entities());
        }
        self.nodes_info.insert(participant, current);
        order_participant_events(events)
    }

    fn get_entity_json_value(
        &self,
        entity_ref: &EntityRef,
    ) -> Result<Option<serde_json::Value>, serde_json::Error> {
        match entity_ref {
            EntityRef::Participant(gid) => self
                .participants
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Writer(gid) => self
                .writers
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Reader(gid) => self
                .readers
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Node(gid, name) => self
                .nodes_info
                .get(gid)
                .and_then(|map| map.get(name))
                .map(serde_json::to_value)
                .transpose(),
        }
    }

    pub async fn treat_admin_query(&self, query: &Query, admin_keyexpr_prefix: &keyexpr) {
        let selector = query.selector();

        // get the list of sub-key expressions that will match the same stored keys than
        // the selector, if those keys had the admin_keyexpr_prefix.
        let sub_kes = selector.key_expr().strip_prefix(admin_keyexpr_prefix);
        if sub_kes.is_empty() {
            tracing::error!("Received query for admin space: '{}' - but it's not prefixed by admin_keyexpr_prefix='{}'", selector, admin_keyexpr_prefix);
            return;
        }

        // For all sub-key expression
        for sub_ke in sub_kes {
            if sub_ke.is_wild() {
                // iterate over all admin space to find matching keys and reply for each
                for (ke, entity_ref) in self.admin_space.iter() {
                    if sub_ke.intersects(ke) {
                        self.send_admin_reply(query, admin_keyexpr_prefix, ke, entity_ref)
                            .await;
                    }
                }
            } else {
                // sub_ke correspond to 1 key - just get it and reply
                if let Some(entity_ref) = self.admin_space.get(sub_ke) {
                    self.send_admin_reply(query, admin_keyexpr_prefix, sub_ke, entity_ref)
                        .await;
                }
            }
        }
    }

    async fn send_admin_reply(
        &self,
        query: &Query,
        admin_keyexpr_prefix: &keyexpr,
        key_expr: &keyexpr,
        entity_ref: &EntityRef,
    ) {
        match self.get_entity_json_value(entity_ref) {
            Ok(Some(v)) => {
                let admin_keyexpr = admin_keyexpr_prefix / key_expr;
                match serde_json::to_vec(&v) {
                    Ok(bytes) => {
                        if let Err(e) = query
                            .reply(admin_keyexpr, ZBytes::from(bytes))
                            .encoding(Encoding::APPLICATION_JSON)
                            .await
                        {
                            tracing::warn!("Error replying to admin query {:?}: {}", query, e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Error transforming JSON to admin query {:?}: {}", query, e);
                    }
                }
            }
            Ok(None) => {
                tracing::error!("INTERNAL ERROR: Dangling {:?} for {}", entity_ref, key_expr)
            }
            Err(e) => {
                tracing::error!("INTERNAL ERROR serializing admin value as JSON: {}", e)
            }
        }
    }
}

// Routes are shared by interface family/name, but their DDS resources also
// depend on type. A same-topic survivor alone does not establish compatibility.
fn interface_signature(event: &ROS2DiscoveryEvent) -> (u8, &str, &str) {
    use ROS2DiscoveryEvent::*;
    match event {
        DiscoveredMsgPub(_, _, v) | UndiscoveredMsgPub(_, _, v) => (0, &v.name, &v.typ),
        DiscoveredMsgSub(_, _, v) | UndiscoveredMsgSub(_, _, v) => (1, &v.name, &v.typ),
        DiscoveredServiceSrv(_, _, v) | UndiscoveredServiceSrv(_, _, v) => (2, &v.name, &v.typ),
        DiscoveredServiceCli(_, _, v) | UndiscoveredServiceCli(_, _, v) => (3, &v.name, &v.typ),
        DiscoveredActionSrv(_, _, v) | UndiscoveredActionSrv(_, _, v) => (4, &v.name, &v.typ),
        DiscoveredActionCli(_, _, v) | UndiscoveredActionCli(_, _, v) => (5, &v.name, &v.typ),
    }
}

fn is_discovery(event: &ROS2DiscoveryEvent) -> bool {
    use ROS2DiscoveryEvent::*;
    matches!(
        event,
        DiscoveredMsgPub(..)
            | DiscoveredMsgSub(..)
            | DiscoveredServiceSrv(..)
            | DiscoveredServiceCli(..)
            | DiscoveredActionSrv(..)
            | DiscoveredActionCli(..)
    )
}

fn push_replacement_events(
    events: &mut Vec<ROS2DiscoveryEvent>,
    withdrawal: Option<ROS2DiscoveryEvent>,
    replacement: Option<ROS2DiscoveryEvent>,
) {
    events.extend(withdrawal.filter(|old| {
        replacement.as_ref().map(interface_signature) != Some(interface_signature(old))
    }));
    events.extend(replacement);
}

fn order_participant_events(events: Vec<ROS2DiscoveryEvent>) -> Vec<ROS2DiscoveryEvent> {
    if !events.iter().any(is_discovery) || events.iter().all(is_discovery) {
        return events;
    }
    // Determine ordering across the whole participant, not one node at a time.
    // Borrow signatures only during classification; no persistent routing index
    // or cloned interface state is needed.
    let mut additions = HashMap::new();
    for event in events.iter().filter(|event| is_discovery(event)) {
        let (family, name, typ) = interface_signature(event);
        additions
            .entry((family, name))
            .and_modify(|(first_type, mixed)| *mixed |= *first_type != typ)
            .or_insert((typ, false));
    }
    let phases: Vec<_> = events
        .iter()
        .map(|event| {
            if is_discovery(event) {
                1
            } else {
                let (family, name, typ) = interface_signature(event);
                if additions
                    .get(&(family, name))
                    .is_some_and(|(new_type, mixed)| *mixed || *new_type != typ)
                {
                    0 // Retire incompatible DDS resources before recreation.
                } else {
                    2 // Keep the route alive during same-type owner transfers.
                }
            }
        })
        .collect();
    let mut ordered = [Vec::new(), Vec::new(), Vec::new()];
    for (event, phase) in events.into_iter().zip(phases) {
        ordered[phase].push(event);
    }
    let [mut replacements, additions, withdrawals] = ordered;
    replacements.extend(additions);
    replacements.extend(withdrawals);
    replacements
}

// Remove any null QoS values from a serde_json::Value
fn remove_null_qos_values(
    value: Result<serde_json::Value, serde_json::Error>,
) -> Result<serde_json::Value, serde_json::Error> {
    match value {
        Ok(value) => match value {
            serde_json::Value::Object(mut obj) => {
                let qos = obj.get_mut("qos");
                if let Some(qos) = qos {
                    if qos.is_object() {
                        qos.as_object_mut().unwrap().retain(|_, v| !v.is_null());
                    }
                }
                Ok(serde_json::Value::Object(obj))
            }
            _ => Ok(value),
        },
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::ros_discovery::NodeEntitiesInfo;
    use cyclors::qos::Qos;
    fn gid(n: u8) -> Gid {
        Gid::from([n; 16])
    }
    fn entity(p: u8, id: u8, writer: bool) -> DdsEntity {
        DdsEntity {
            key: gid(id),
            participant_key: gid(p),
            topic_name: if writer {
                "rr/get_frameReply"
            } else {
                "rq/get_frameRequest"
            }
            .into(),
            type_name: if writer {
                "example_interfaces::srv::dds_::AddTwoInts_Response_"
            } else {
                "example_interfaces::srv::dds_::AddTwoInts_Request_"
            }
            .into(),
            _type_info: None,
            keyless: true,
            qos: Qos::default(),
        }
    }
    fn graph(p: u8, reader: u8, writer: u8) -> ParticipantEntitiesInfo {
        let mut info = ParticipantEntitiesInfo::new(gid(p));
        let mut node = NodeEntitiesInfo::new("/".into(), "camera".into());
        if reader != 0 {
            node.reader_gid_seq.insert(gid(reader));
        }
        if writer != 0 {
            node.writer_gid_seq.insert(gid(writer));
        }
        info.node_entities_info_seq.insert(node.full_name(), node);
        info
    }
    fn initial() -> DiscoveredEntities {
        let mut d = DiscoveredEntities::default();
        d.add_participant(DdsParticipant {
            key: gid(1),
            qos: Qos::default(),
        });
        d.add_reader(entity(1, 11, false));
        d.add_writer(entity(1, 12, true));
        d.update_participant_info(graph(1, 11, 12));
        d
    }
    fn complete(d: &DiscoveredEntities) -> bool {
        d.nodes_info
            .get(&gid(1))
            .and_then(|n| n.get("/camera"))
            .and_then(|n| n.service_srv.get("/get_frame"))
            .is_some_and(|s| s.is_complete())
    }
    #[test]
    fn replacement_does_not_lose_new_counterpart_on_old_dispose() {
        let mut d = initial();
        assert!(complete(&d));
        d.update_participant_info(graph(1, 13, 14));
        d.add_writer(entity(1, 14, true));
        d.remove_reader(&gid(11));
        d.add_reader(entity(1, 13, false));
        d.remove_writer(&gid(12));
        assert!(
            complete(&d),
            "new reply writer was lost by old request reader disposal"
        );
    }
    #[test]
    fn ros_membership_removal_does_not_wait_for_dds_dispose() {
        let mut d = initial();
        d.update_participant_info(graph(1, 0, 0));
        assert!(
            !complete(&d),
            "obsolete endpoints survived authoritative node snapshot"
        );
    }
    #[test]
    fn repeated_snapshot_has_bounded_pending_endpoints() {
        let mut d = DiscoveredEntities::default();
        d.add_participant(DdsParticipant {
            key: gid(1),
            qos: Qos::default(),
        });
        for _ in 0..100 {
            d.update_participant_info(graph(1, 13, 14));
        }
        let node = &d.nodes_info[&gid(1)]["/camera"];
        assert_eq!(node.undiscovered_reader.len(), 1);
        assert_eq!(node.undiscovered_writer.len(), 1);
    }
    #[test]
    fn participant_retirement_releases_cached_discovery() {
        let mut d = initial();
        d.remove_participant(&gid(1));
        assert!(d.ros_participant_info.is_empty());
        assert!(d.readers.is_empty());
        assert!(d.writers.is_empty());
        assert!(d.nodes_info.is_empty());
    }

    fn interface_endpoints(action: bool, client: bool, copy: u8) -> Vec<(bool, DdsEntity)> {
        let topics: &[(&str, &str)] = if action {
            &[
                (
                    "rq/test/_action/send_goalRequest",
                    "Fibonacci_SendGoal_Request_",
                ),
                (
                    "rr/test/_action/send_goalReply",
                    "Fibonacci_SendGoal_Response_",
                ),
                ("rq/test/_action/cancel_goalRequest", "CancelGoal_Request_"),
                ("rr/test/_action/cancel_goalReply", "CancelGoal_Response_"),
                (
                    "rq/test/_action/get_resultRequest",
                    "Fibonacci_GetResult_Request_",
                ),
                (
                    "rr/test/_action/get_resultReply",
                    "Fibonacci_GetResult_Response_",
                ),
                ("rt/test/_action/feedback", "Fibonacci_FeedbackMessage_"),
                ("rt/test/_action/status", "GoalStatusArray_"),
            ]
        } else {
            &[
                ("rq/testRequest", "AddTwoInts_Request_"),
                ("rr/testReply", "AddTwoInts_Response_"),
            ]
        };
        topics
            .iter()
            .enumerate()
            .map(|(index, (topic, typ))| {
                let writer = topic.starts_with("rq/") == client;
                let mut endpoint = entity(1, 20 + copy * 10 + index as u8, writer);
                endpoint.topic_name = (*topic).into();
                endpoint.type_name = format!(
                    "example_interfaces::{}::dds_::{typ}",
                    if action { "action" } else { "srv" }
                );
                (writer, endpoint)
            })
            .collect()
    }

    fn add_endpoint(
        d: &mut DiscoveredEntities,
        writer: bool,
        endpoint: DdsEntity,
    ) -> Vec<ROS2DiscoveryEvent> {
        if writer {
            d.add_writer(endpoint)
        } else {
            d.add_reader(endpoint)
        }
    }
    fn remove_endpoint(
        d: &mut DiscoveredEntities,
        writer: bool,
        gid: &Gid,
    ) -> Vec<ROS2DiscoveryEvent> {
        if writer {
            d.remove_writer(gid)
        } else {
            d.remove_reader(gid)
        }
    }
    fn discover_interface(endpoints: &[(bool, DdsEntity)]) -> DiscoveredEntities {
        let mut d = DiscoveredEntities::default();
        d.add_participant(DdsParticipant {
            key: gid(1),
            qos: Qos::default(),
        });
        let mut info = graph(1, 0, 0);
        let node = info.node_entities_info_seq.get_mut("/camera").unwrap();
        for (writer, endpoint) in endpoints {
            if *writer {
                node.writer_gid_seq.insert(endpoint.key);
            } else {
                node.reader_gid_seq.insert(endpoint.key);
            }
            add_endpoint(&mut d, *writer, endpoint.clone());
        }
        assert_eq!(d.update_participant_info(info).len(), 1);
        d
    }

    #[test]
    fn every_service_and_action_component_can_return_without_a_new_ros_snapshot() {
        for action in [false, true] {
            for client in [false, true] {
                let endpoints = interface_endpoints(action, client, 0);
                let mut d = discover_interface(&endpoints);
                for (writer, endpoint) in endpoints {
                    assert_eq!(remove_endpoint(&mut d, writer, &endpoint.key).len(), 1);
                    assert!(remove_endpoint(&mut d, writer, &endpoint.key).is_empty());
                    assert_eq!(
                        add_endpoint(&mut d, writer, endpoint.clone()).len(),
                        1,
                        "removing {} lost its other components",
                        endpoint.topic_name
                    );
                    assert!(add_endpoint(&mut d, writer, endpoint).is_empty());
                    let node = &d.nodes_info[&gid(1)]["/camera"];
                    assert!(node.undiscovered_reader.is_empty());
                    assert!(node.undiscovered_writer.is_empty());
                }
            }
        }
    }

    #[test]
    fn disposing_one_of_multiple_service_or_action_endpoints_preserves_the_route() {
        for action in [false, true] {
            for client in [false, true] {
                let mut endpoints = interface_endpoints(action, client, 0);
                let replacements = interface_endpoints(action, client, 1);
                endpoints.extend(replacements.clone());
                let mut d = discover_interface(&endpoints);
                // Snapshot construction selects the higher GIDs. Remove those:
                // each disposal must swap in the surviving lower GID with
                // exactly one re-announcement and no withdrawal.
                for (writer, endpoint) in replacements {
                    let events = remove_endpoint(&mut d, writer, &endpoint.key);
                    assert!(
                        matches!(
                            events.as_slice(),
                            [ROS2DiscoveryEvent::DiscoveredServiceSrv(..)]
                                | [ROS2DiscoveryEvent::DiscoveredServiceCli(..)]
                                | [ROS2DiscoveryEvent::DiscoveredActionSrv(..)]
                                | [ROS2DiscoveryEvent::DiscoveredActionCli(..)]
                        ),
                        "expected one swap announcement for {}: {events:?}",
                        endpoint.topic_name
                    );
                }
                // Removing the survivors withdraws the route exactly once, and
                // the fully cleared interface entry is dropped from the node.
                let mut withdrawals = 0;
                for (writer, endpoint) in interface_endpoints(action, client, 0) {
                    let events = remove_endpoint(&mut d, writer, &endpoint.key);
                    withdrawals += events.len();
                    assert!(
                        events.iter().all(|event| matches!(
                            event,
                            ROS2DiscoveryEvent::UndiscoveredServiceSrv(..)
                                | ROS2DiscoveryEvent::UndiscoveredServiceCli(..)
                                | ROS2DiscoveryEvent::UndiscoveredActionSrv(..)
                                | ROS2DiscoveryEvent::UndiscoveredActionCli(..)
                        )),
                        "unexpected event: {events:?}"
                    );
                }
                assert_eq!(withdrawals, 1);
                let node = &d.nodes_info[&gid(1)]["/camera"];
                assert!(node.service_srv.is_empty());
                assert!(node.service_cli.is_empty());
                assert!(node.action_srv.is_empty());
                assert!(node.action_cli.is_empty());
            }
        }
    }

    #[test]
    fn disposing_an_untracked_endpoint_does_not_rewrite_the_interface() {
        for action in [false, true] {
            for client in [false, true] {
                let mut endpoints = interface_endpoints(action, client, 0);
                endpoints.extend(interface_endpoints(action, client, 1));
                let mut d = discover_interface(&endpoints);
                // Snapshot selection references the higher copy-1 GIDs, so the
                // copy-0 endpoints back no interface component. Disposing them
                // must be silent and leave the live interface untouched.
                for (writer, endpoint) in interface_endpoints(action, client, 0) {
                    let events = remove_endpoint(&mut d, writer, &endpoint.key);
                    assert!(
                        events.is_empty(),
                        "untracked disposal of {} rewrote the interface: {events:?}",
                        endpoint.topic_name
                    );
                }
                let node = &d.nodes_info[&gid(1)]["/camera"];
                let complete = node
                    .service_srv
                    .values()
                    .filter(|s| s.is_complete())
                    .count()
                    + node
                        .service_cli
                        .values()
                        .filter(|s| s.is_complete())
                        .count()
                    + node.action_srv.values().filter(|s| s.is_complete()).count()
                    + node.action_cli.values().filter(|s| s.is_complete()).count();
                assert_eq!(complete, 1);
            }
        }
    }

    fn topic_entity(id: u8, writer: bool) -> DdsEntity {
        let mut endpoint = entity(1, id, writer);
        endpoint.topic_name = "rt/image".into();
        endpoint.type_name = "sensor_msgs::msg::dds_::Image_".into();
        endpoint
    }

    fn event_details(event: &ROS2DiscoveryEvent) -> (bool, Gid, &str, &str, &str) {
        use ROS2DiscoveryEvent::*;
        match event {
            DiscoveredMsgPub(p, n, v) => (true, *p, n, &v.name, &v.typ),
            UndiscoveredMsgPub(p, n, v) => (false, *p, n, &v.name, &v.typ),
            DiscoveredMsgSub(p, n, v) => (true, *p, n, &v.name, &v.typ),
            UndiscoveredMsgSub(p, n, v) => (false, *p, n, &v.name, &v.typ),
            DiscoveredServiceSrv(p, n, v) => (true, *p, n, &v.name, &v.typ),
            UndiscoveredServiceSrv(p, n, v) => (false, *p, n, &v.name, &v.typ),
            DiscoveredServiceCli(p, n, v) => (true, *p, n, &v.name, &v.typ),
            UndiscoveredServiceCli(p, n, v) => (false, *p, n, &v.name, &v.typ),
            DiscoveredActionSrv(p, n, v) => (true, *p, n, &v.name, &v.typ),
            UndiscoveredActionSrv(p, n, v) => (false, *p, n, &v.name, &v.typ),
            DiscoveredActionCli(p, n, v) => (true, *p, n, &v.name, &v.typ),
            UndiscoveredActionCli(p, n, v) => (false, *p, n, &v.name, &v.typ),
        }
    }

    fn discovered_components(event: &ROS2DiscoveryEvent) -> Vec<Gid> {
        use ROS2DiscoveryEvent::*;
        match event {
            DiscoveredServiceSrv(_, _, v) => vec![v.entities.req_reader, v.entities.rep_writer],
            DiscoveredServiceCli(_, _, v) => vec![v.entities.req_writer, v.entities.rep_reader],
            DiscoveredActionSrv(_, _, v) => vec![
                v.entities.send_goal.req_reader,
                v.entities.send_goal.rep_writer,
                v.entities.cancel_goal.req_reader,
                v.entities.cancel_goal.rep_writer,
                v.entities.get_result.req_reader,
                v.entities.get_result.rep_writer,
                v.entities.feedback_writer,
                v.entities.status_writer,
            ],
            DiscoveredActionCli(_, _, v) => vec![
                v.entities.send_goal.req_writer,
                v.entities.send_goal.rep_reader,
                v.entities.cancel_goal.req_writer,
                v.entities.cancel_goal.rep_reader,
                v.entities.get_result.req_writer,
                v.entities.get_result.rep_reader,
                v.entities.feedback_reader,
                v.entities.status_reader,
            ],
            _ => panic!("expected a complete service/action discovery: {event:?}"),
        }
    }

    fn changed_type(mut endpoints: Vec<(bool, DdsEntity)>) -> Vec<(bool, DdsEntity)> {
        for (_, endpoint) in &mut endpoints {
            endpoint.type_name = endpoint
                .type_name
                .replace("AddTwoInts", "SetBool")
                .replace("Fibonacci", "OtherAction")
                .replace(
                    "sensor_msgs::msg::dds_::Image_",
                    "std_msgs::msg::dds_::String_",
                );
        }
        endpoints
    }

    #[test]
    fn incompatible_topic_survivor_withdraws_before_rediscovery() {
        for writer in [false, true] {
            let old = topic_entity(11, writer);
            let new = changed_type(vec![(writer, topic_entity(12, writer))])
                .pop()
                .unwrap()
                .1;
            let mut d = discover_interface(&[(writer, old.clone())]);
            assert!(add_endpoint(&mut d, writer, new.clone()).is_empty());
            let mut info = d.ros_participant_info[&gid(1)].clone();
            let node = info.node_entities_info_seq.get_mut("/camera").unwrap();
            if writer {
                node.writer_gid_seq.insert(new.key);
            } else {
                node.reader_gid_seq.insert(new.key);
            }
            // Both types are present in one real ROS node. The second type is
            // ignored until the final endpoint of the original type disappears.
            assert!(d.update_participant_info(info.clone()).is_empty());
            let events = remove_endpoint(&mut d, writer, &old.key);
            assert_eq!(
                events.iter().map(event_details).collect::<Vec<_>>(),
                [
                    (false, gid(1), "/camera", "/image", "sensor_msgs/msg/Image"),
                    (true, gid(1), "/camera", "/image", "std_msgs/msg/String"),
                ],
                "writer={writer}: {events:?}"
            );
            let node = info.node_entities_info_seq.get_mut("/camera").unwrap();
            if writer {
                node.writer_gid_seq.remove(&old.key);
                assert_eq!(
                    d.nodes_info[&gid(1)]["/camera"].msg_pub["/image"].writers,
                    [new.key].into()
                );
            } else {
                node.reader_gid_seq.remove(&old.key);
                assert_eq!(
                    d.nodes_info[&gid(1)]["/camera"].msg_sub["/image"].readers,
                    [new.key].into()
                );
            }
            assert!(d.update_participant_info(info).is_empty());
        }
    }

    #[test]
    fn incompatible_service_and_action_survivors_withdraw_the_old_type() {
        for action in [false, true] {
            for client in [false, true] {
                let old = interface_endpoints(action, client, 1);
                let new = changed_type(interface_endpoints(action, client, 0));
                for (index, (writer, endpoint)) in old.iter().enumerate() {
                    if endpoint.type_name == new[index].1.type_name {
                        continue; // Action status/cancel components have a shared type.
                    }
                    let mut endpoints = new.clone();
                    endpoints.extend(old.clone());
                    let mut d = discover_interface(&endpoints);
                    let events = remove_endpoint(&mut d, *writer, &endpoint.key);
                    let old_type = if action {
                        "example_interfaces/action/Fibonacci"
                    } else {
                        "example_interfaces/srv/AddTwoInts"
                    };
                    let new_type = if action {
                        "example_interfaces/action/OtherAction"
                    } else {
                        "example_interfaces/srv/SetBool"
                    };
                    assert_eq!(
                        events.iter().map(event_details).collect::<Vec<_>>(),
                        [
                            (false, gid(1), "/camera", "/test", old_type),
                            (true, gid(1), "/camera", "/test", new_type),
                        ],
                        "action={action}, client={client}, component={index}: {events:?}"
                    );
                    let mut expected: Vec<_> = old.iter().map(|(_, e)| e.key).collect();
                    expected[index] = new[index].1.key;
                    assert_eq!(discovered_components(&events[1]), expected);
                    // A different-type handover must not erase the other
                    // already-known components while retiring the old route.
                }
            }
        }
    }

    #[test]
    fn compatible_service_and_action_survivors_take_precedence_over_other_types() {
        for action in [false, true] {
            for client in [false, true] {
                let compatible = interface_endpoints(action, client, 0);
                let incompatible = changed_type(interface_endpoints(action, client, 1));
                let selected = interface_endpoints(action, client, 2);
                for (index, (writer, endpoint)) in selected.iter().enumerate() {
                    if endpoint.type_name == incompatible[index].1.type_name {
                        continue;
                    }
                    let mut endpoints = compatible.clone();
                    endpoints.push(incompatible[index].clone());
                    endpoints.extend(selected.clone());
                    let mut d = discover_interface(&endpoints);
                    let events = remove_endpoint(&mut d, *writer, &endpoint.key);
                    let typ = if action {
                        "example_interfaces/action/Fibonacci"
                    } else {
                        "example_interfaces/srv/AddTwoInts"
                    };
                    assert_eq!(
                        events.iter().map(event_details).collect::<Vec<_>>(),
                        [(true, gid(1), "/camera", "/test", typ)],
                        "action={action}, client={client}, component={index}: {events:?}"
                    );
                    let mut expected: Vec<_> = selected.iter().map(|(_, e)| e.key).collect();
                    expected[index] = compatible[index].1.key;
                    assert_eq!(discovered_components(&events[0]), expected);
                    // The actual retained component, not just the advertised
                    // type, must come from the compatible endpoint.
                }
            }
        }
    }

    fn participant_handoff(
        old: Vec<(bool, DdsEntity)>,
        new: Vec<(bool, DdsEntity)>,
        type_change: bool,
    ) {
        let mut d = DiscoveredEntities::default();
        d.add_participant(DdsParticipant {
            key: gid(1),
            qos: Qos::default(),
        });
        for (writer, endpoint) in old.iter().chain(&new) {
            add_endpoint(&mut d, *writer, endpoint.clone());
        }
        let mut next = ParticipantEntitiesInfo::new(gid(1));
        for name in ["one", "two"] {
            let node = NodeEntitiesInfo::new("/".into(), name.into());
            next.node_entities_info_seq.insert(node.full_name(), node);
        }
        // Assign the old owner to the actual first iterated node. Updating only
        // values below preserves that iteration order, making c04 fail without
        // relying on a particular randomized HashMap seed.
        let names: Vec<_> = next.node_entities_info_seq.keys().cloned().collect();
        let old_name = &names[0];
        let new_name = &names[1];
        let mut previous = next.clone();
        for (info, name, endpoints) in
            [(&mut previous, old_name, &old), (&mut next, new_name, &new)]
        {
            let node = info.node_entities_info_seq.get_mut(name).unwrap();
            for (writer, endpoint) in endpoints {
                if *writer {
                    node.writer_gid_seq.insert(endpoint.key);
                } else {
                    node.reader_gid_seq.insert(endpoint.key);
                }
            }
        }
        let initial = d.update_participant_info(previous);
        assert_eq!(initial.len(), 1);
        let old_type = event_details(&initial[0]).4;
        let events = d.update_participant_info(next.clone());
        assert_eq!(events.len(), 2, "{events:?}");
        let details: Vec<_> = events.iter().map(event_details).collect();
        let (withdrawal, addition) = if type_change { (0, 1) } else { (1, 0) };
        assert_eq!(details[withdrawal].0, false, "{events:?}");
        assert_eq!(details[withdrawal].1, gid(1));
        assert_eq!(details[withdrawal].2, old_name);
        assert_eq!(details[withdrawal].4, old_type);
        assert_eq!(details[addition].0, true, "{events:?}");
        assert_eq!(details[addition].1, gid(1));
        assert_eq!(details[addition].2, new_name);
        assert_eq!(details[addition].4 == old_type, !type_change);
        assert!(d.update_participant_info(next).is_empty());
    }

    #[test]
    fn transfers_between_existing_nodes_preserve_same_type_routes() {
        for writer in [false, true] {
            participant_handoff(
                vec![(writer, topic_entity(11, writer))],
                vec![(writer, topic_entity(12, writer))],
                false,
            );
        }
        for action in [false, true] {
            for client in [false, true] {
                participant_handoff(
                    interface_endpoints(action, client, 0),
                    interface_endpoints(action, client, 1),
                    false,
                );
            }
        }
    }

    #[test]
    fn type_changes_between_nodes_withdraw_before_discovery() {
        for writer in [false, true] {
            participant_handoff(
                vec![(writer, topic_entity(11, writer))],
                changed_type(vec![(writer, topic_entity(12, writer))]),
                true,
            );
        }
        for action in [false, true] {
            for client in [false, true] {
                participant_handoff(
                    interface_endpoints(action, client, 0),
                    changed_type(interface_endpoints(action, client, 1)),
                    true,
                );
            }
        }
    }

    #[test]
    fn topic_route_survives_until_its_last_endpoint_is_disposed() {
        let mut d = DiscoveredEntities::default();
        d.add_participant(DdsParticipant {
            key: gid(1),
            qos: Qos::default(),
        });
        let mut info = graph(1, 0, 0);
        let node = info.node_entities_info_seq.get_mut("/camera").unwrap();
        for id in [31, 32] {
            node.writer_gid_seq.insert(gid(id));
        }
        for id in [41, 42] {
            node.reader_gid_seq.insert(gid(id));
        }
        for id in [31, 32] {
            assert!(d.add_writer(topic_entity(id, true)).is_empty());
        }
        for id in [41, 42] {
            assert!(d.add_reader(topic_entity(id, false)).is_empty());
        }
        assert_eq!(d.update_participant_info(info).len(), 2);
        // Disposing one of two endpoints keeps the route and stays silent.
        assert!(d.remove_writer(&gid(31)).is_empty());
        assert!(d.remove_reader(&gid(41)).is_empty());
        // Disposing the last endpoint withdraws the route, and only then.
        let events = d.remove_writer(&gid(32));
        assert!(
            matches!(
                events.as_slice(),
                [ROS2DiscoveryEvent::UndiscoveredMsgPub(..)]
            ),
            "{events:?}"
        );
        let events = d.remove_reader(&gid(42));
        assert!(
            matches!(
                events.as_slice(),
                [ROS2DiscoveryEvent::UndiscoveredMsgSub(..)]
            ),
            "{events:?}"
        );
        let node = &d.nodes_info[&gid(1)]["/camera"];
        assert!(node.msg_pub.is_empty() && node.msg_sub.is_empty());
        // A returning endpoint re-announces without a new ROS snapshot.
        let events = d.add_writer(topic_entity(32, true));
        assert!(
            matches!(
                events.as_slice(),
                [ROS2DiscoveryEvent::DiscoveredMsgPub(..)]
            ),
            "{events:?}"
        );
        let events = d.add_reader(topic_entity(42, false));
        assert!(
            matches!(
                events.as_slice(),
                [ROS2DiscoveryEvent::DiscoveredMsgSub(..)]
            ),
            "{events:?}"
        );
        let node = &d.nodes_info[&gid(1)]["/camera"];
        assert_eq!(node.undiscovered_writer, [gid(31)]);
        assert_eq!(node.undiscovered_reader, [gid(41)]);
    }

    #[test]
    fn node_rename_discovers_the_replacement_before_withdrawing_its_predecessor() {
        let mut d = initial();
        let mut info = ParticipantEntitiesInfo::new(gid(1));
        let mut node = NodeEntitiesInfo::new("/".into(), "camera2".into());
        node.reader_gid_seq.insert(gid(11));
        node.writer_gid_seq.insert(gid(12));
        info.node_entities_info_seq.insert(node.full_name(), node);
        // The route is keyed by interface name: its new owner must be
        // announced before the vanished node's withdrawal can empty it.
        let events = d.update_participant_info(info);
        assert!(
            matches!(
                events.as_slice(),
                [
                    ROS2DiscoveryEvent::DiscoveredServiceSrv(_, discovered, _),
                    ROS2DiscoveryEvent::UndiscoveredServiceSrv(_, withdrawn, _),
                ] if discovered.as_str() == "/camera2" && withdrawn.as_str() == "/camera"
            ),
            "{events:?}"
        );
    }

    fn permutations(items: &mut [u8], start: usize, run: &mut impl FnMut(&[u8])) {
        if start == items.len() {
            run(items);
            return;
        }
        for i in start..items.len() {
            items.swap(start, i);
            permutations(items, start + 1, run);
            items.swap(start, i);
        }
    }

    #[test]
    fn replacement_converges_in_all_120_discovery_orders() {
        let mut count = 0;
        permutations(&mut [0, 1, 2, 3, 4], 0, &mut |order| {
            let mut d = initial();
            for op in order {
                match op {
                    0 => {
                        d.update_participant_info(graph(1, 13, 14));
                    }
                    1 => {
                        d.add_reader(entity(1, 13, false));
                    }
                    2 => {
                        d.add_writer(entity(1, 14, true));
                    }
                    3 => {
                        d.remove_reader(&gid(11));
                    }
                    4 => {
                        d.remove_writer(&gid(12));
                    }
                    _ => unreachable!(),
                }
            }
            assert!(complete(&d), "order: {order:?}");
            let service = &d.nodes_info[&gid(1)]["/camera"].service_srv["/get_frame"];
            assert_eq!(service.entities.req_reader, gid(13));
            assert_eq!(service.entities.rep_writer, gid(14));
            assert!(
                d.update_participant_info(graph(1, 13, 14)).is_empty(),
                "non-idempotent snapshot"
            );
            count += 1;
        });
        assert_eq!(count, 120);
    }

    #[test]
    fn first_discovery_converges_in_all_24_input_orders() {
        permutations(&mut [0, 1, 2, 3], 0, &mut |order| {
            let mut d = DiscoveredEntities::default();
            for op in order {
                match op {
                    0 => {
                        d.add_participant(DdsParticipant {
                            key: gid(1),
                            qos: Qos::default(),
                        });
                    }
                    1 => {
                        d.add_reader(entity(1, 11, false));
                    }
                    2 => {
                        d.add_writer(entity(1, 12, true));
                    }
                    3 => {
                        d.update_participant_info(graph(1, 11, 12));
                    }
                    _ => unreachable!(),
                }
            }
            assert!(complete(&d), "order: {order:?}");
        });
    }

    #[test]
    fn late_dispose_of_same_named_predecessor_preserves_replacement() {
        let mut d = initial();
        d.add_participant(DdsParticipant {
            key: gid(2),
            qos: Qos::default(),
        });
        d.add_reader(entity(2, 21, false));
        d.add_writer(entity(2, 22, true));
        d.update_participant_info(graph(2, 21, 22));
        let removed = d.remove_participant(&gid(1));
        assert!(
            matches!(removed.as_slice(), [ROS2DiscoveryEvent::UndiscoveredServiceSrv(p, _, _)] if *p == gid(1))
        );
        assert!(d.remove_reader(&gid(11)).is_empty());
        assert!(d.remove_writer(&gid(12)).is_empty());
        assert!(d.nodes_info[&gid(2)]["/camera"].service_srv["/get_frame"].is_complete());
        assert_eq!(d.nodes_info.len(), 1);
    }
}

#[cfg(test)]
mod cpu_benchmark {
    use super::*;
    use crate::dds_discovery::DDSDiscoveryEvent;
    use crate::ros_discovery::NodeEntitiesInfo;
    use cyclors::qos::Qos;
    use std::time::Instant;

    fn id(n: usize) -> Gid {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&(n as u64).to_le_bytes());
        Gid::from(bytes)
    }
    fn endpoint(i: usize, writer: bool) -> DdsEntity {
        DdsEntity {
            key: id(100 + 2 * i + usize::from(writer)),
            participant_key: id(1),
            topic_name: if writer {
                format!("rq/bench/service_{i}Request")
            } else {
                format!("rr/bench/service_{i}Reply")
            },
            type_name: if writer {
                "example_interfaces::srv::dds_::AddTwoInts_Request_"
            } else {
                "example_interfaces::srv::dds_::AddTwoInts_Response_"
            }
            .into(),
            _type_info: None,
            keyless: true,
            qos: Qos::default(),
        }
    }
    fn graph(n: usize, nodes: usize) -> ParticipantEntitiesInfo {
        let mut info = ParticipantEntitiesInfo::new(id(1));
        for i in 0..n {
            let name = format!("/node_{}", i % nodes);
            let node = info.node_entities_info_seq.entry(name).or_insert_with(|| {
                NodeEntitiesInfo::new("/".into(), format!("node_{}", i % nodes))
            });
            node.reader_gid_seq.insert(id(100 + 2 * i));
            node.writer_gid_seq.insert(id(101 + 2 * i));
        }
        info
    }
    fn events(start: usize, end: usize) -> Vec<DDSDiscoveryEvent> {
        (start..end)
            .flat_map(|i| {
                [
                    DDSDiscoveryEvent::DiscoveredPublication {
                        entity: endpoint(i, true),
                    },
                    DDSDiscoveryEvent::DiscoveredSubscription {
                        entity: endpoint(i, false),
                    },
                ]
            })
            .collect()
    }
    fn apply(d: &mut DiscoveredEntities, input: Vec<DDSDiscoveryEvent>) -> usize {
        input
            .into_iter()
            .map(|event| d.apply_dds_event(event).len())
            .sum()
    }
    #[test]
    #[ignore = "release-mode discovery CPU comparison; see tests/lifecycle/README.md"]
    fn discovery_churn_benchmark() {
        for nodes in [1, 231] {
            for (base, added) in [
                (0, 250),
                (0, 500),
                (0, 2000),
                (500, 150),
                (1850, 150),
                (1850, 300),
            ] {
                for repeat in 0..3 {
                    let mut d = DiscoveredEntities::default();
                    d.add_participant(DdsParticipant {
                        key: id(1),
                        qos: Qos::default(),
                    });
                    apply(&mut d, events(0, base));
                    d.update_participant_info(graph(base + added, nodes));
                    let input = events(base, base + added);
                    let start = Instant::now();
                    let emitted = std::hint::black_box(apply(&mut d, input));
                    let elapsed_us = start.elapsed().as_micros();
                    let remove_start = Instant::now();
                    for i in base..base + added {
                        d.apply_dds_event(DDSDiscoveryEvent::UndiscoveredPublication {
                            key: id(101 + 2 * i),
                        });
                        d.apply_dds_event(DDSDiscoveryEvent::UndiscoveredSubscription {
                            key: id(100 + 2 * i),
                        });
                    }
                    let remove_us = remove_start.elapsed().as_micros();
                    let complete: usize = d.nodes_info[&id(1)]
                        .values()
                        .map(|n| n.service_cli.values().filter(|s| s.is_complete()).count())
                        .sum();
                    assert_eq!(complete, base);
                    println!("BENCH {{\"nodes\":{nodes},\"base_services\":{base},\"added_services\":{added},\"repeat\":{repeat},\"add_us\":{elapsed_us},\"remove_us\":{remove_us},\"emitted\":{emitted},\"complete\":{complete}}}");
                }
            }
        }
    }
}
