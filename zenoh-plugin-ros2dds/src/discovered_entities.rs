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
    collections::{BTreeSet, HashMap, HashSet},
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
    writers: Endpoints,
    readers: Endpoints,
    ros_participant_info: HashMap<Gid, ParticipantEntitiesInfo>,
    nodes_info: HashMap<Gid, HashMap<String, NodeInfo>>,
    admin_space: HashMap<OwnedKeyExpr, EntityRef>,
}

/// DDS metadata has one owner; the topic index contains only its keys.
/// Removal must inspect competing endpoints, not scan unrelated participants.
#[derive(Default)]
struct Endpoints {
    entities: HashMap<Gid, DdsEntity>,
    topics: HashMap<Gid, HashMap<String, BTreeSet<Gid>>>,
}

enum EndpointUpdate<'a> {
    Metadata,
    Identity {
        current: &'a DdsEntity,
        previous: Option<(Gid, String)>,
    },
}

impl Endpoints {
    fn insert(&mut self, entity: DdsEntity) -> EndpointUpdate<'_> {
        let key = entity.key;
        if let Some(previous) = self.entities.get_mut(&key) {
            if previous.participant_key == entity.participant_key
                && previous.topic_name == entity.topic_name
            {
                *previous = entity;
                return EndpointUpdate::Metadata;
            }
        }
        let previous = self
            .remove(&key)
            .map(|previous| (previous.participant_key, previous.topic_name));
        self.topics
            .entry(entity.participant_key)
            .or_default()
            .entry(entity.topic_name.clone())
            .or_default()
            .insert(key);
        let current = self.entities.entry(key).or_insert(entity);
        EndpointUpdate::Identity { current, previous }
    }

    fn remove(&mut self, key: &Gid) -> Option<DdsEntity> {
        let entity = self.entities.remove(key)?;
        let topics = self.topics.get_mut(&entity.participant_key).unwrap();
        let members = topics.get_mut(&entity.topic_name).unwrap();
        assert!(members.remove(key));
        if members.is_empty() {
            topics.remove(&entity.topic_name);
        }
        if topics.is_empty() {
            self.topics.remove(&entity.participant_key);
        }
        Some(entity)
    }

    fn remove_participant(&mut self, participant: &Gid) {
        if let Some(topics) = self.topics.remove(participant) {
            for key in topics.into_values().flatten() {
                self.entities.remove(&key);
            }
        }
    }

    fn on_topic(
        &self,
        participant: &Gid,
        topic: &str,
    ) -> impl DoubleEndedIterator<Item = &DdsEntity> {
        self.topics
            .get(participant)
            .and_then(|topics| topics.get(topic))
            .into_iter()
            .flatten()
            .map(|key| &self.entities[key])
    }

    fn replacement(&self, removed: &DdsEntity, membership: &HashSet<Gid>) -> Option<&DdsEntity> {
        let mut fallback = None;
        // Reverse GID order preserves max(type_matches, GID), but a compatible
        // survivor needs no inspection of the rest of a densely shared topic.
        for candidate in self
            .on_topic(&removed.participant_key, &removed.topic_name)
            .rev()
        {
            if membership.contains(&candidate.key) {
                if candidate.type_name == removed.type_name {
                    return Some(candidate);
                }
                if fallback.is_none() {
                    fallback = Some(candidate);
                }
            }
        }
        fallback
    }

    fn get(&self, key: &Gid) -> Option<&DdsEntity> {
        self.entities.get(key)
    }

    fn contains_key(&self, key: &Gid) -> bool {
        self.entities.contains_key(key)
    }

    fn keys(&self) -> impl Iterator<Item = &Gid> {
        self.entities.keys()
    }

    fn len(&self) -> usize {
        self.entities.len()
    }
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
        self.writers.remove_participant(gid);
        self.readers.remove_participant(gid);
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
        if let EndpointUpdate::Identity { current, previous } = self.writers.insert(writer) {
            if let Some((participant, topic)) = previous {
                self.admin_space.remove(
                    &keformat!(
                        ke_admin_writer::formatter(),
                        pgid = participant,
                        wgid = current.key,
                        topic = topic
                    )
                    .unwrap(),
                );
            }
            self.admin_space.insert(
                keformat!(
                    ke_admin_writer::formatter(),
                    pgid = current.participant_key,
                    wgid = current.key,
                    topic = &current.topic_name
                )
                .unwrap(),
                EntityRef::Writer(current.key),
            );
        }
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
                        match self.writers.replacement(&writer, membership) {
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
        if let EndpointUpdate::Identity { current, previous } = self.readers.insert(reader) {
            if let Some((participant, topic)) = previous {
                self.admin_space.remove(
                    &keformat!(
                        ke_admin_reader::formatter(),
                        pgid = participant,
                        wgid = current.key,
                        topic = topic
                    )
                    .unwrap(),
                );
            }
            self.admin_space.insert(
                keformat!(
                    ke_admin_reader::formatter(),
                    pgid = current.participant_key,
                    wgid = current.key,
                    topic = &current.topic_name
                )
                .unwrap(),
                EntityRef::Reader(current.key),
            );
        }
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
                        match self.readers.replacement(&reader, membership) {
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
        assert!(d.readers.entities.is_empty());
        assert!(d.writers.entities.is_empty());
        assert!(d.readers.topics.is_empty());
        assert!(d.writers.topics.is_empty());
        assert!(d.nodes_info.is_empty());
    }

    #[test]
    fn endpoint_index_tracks_metadata_replacement_and_releases_empty_buckets() {
        let mut endpoints = Endpoints::default();
        let first = entity(1, 20, true);
        let second = entity(1, 21, true);
        let topic = first.topic_name.clone();
        endpoints.insert(first);
        endpoints.insert(second);
        assert_eq!(endpoints.on_topic(&gid(1), &topic).count(), 2);

        let mut replacement = entity(2, 20, true);
        replacement.topic_name = "rq/newRequest".into();
        for _ in 0..100 {
            endpoints.insert(replacement.clone());
        }
        replacement.type_name = "ChangedRequest_".into();
        endpoints.insert(replacement);
        assert_eq!(
            endpoints
                .on_topic(&gid(2), "rq/newRequest")
                .next()
                .unwrap()
                .type_name,
            "ChangedRequest_"
        );
        assert_eq!(endpoints.len(), 2);
        assert_eq!(
            endpoints
                .on_topic(&gid(1), &topic)
                .map(|e| e.key)
                .collect::<Vec<_>>(),
            vec![gid(21)]
        );
        assert_eq!(
            endpoints
                .on_topic(&gid(2), "rq/newRequest")
                .map(|e| e.key)
                .collect::<Vec<_>>(),
            vec![gid(20)]
        );

        endpoints.remove_participant(&gid(1));
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints.topics.len(), 1);
        assert!(endpoints.remove(&gid(20)).is_some());
        assert!(endpoints.remove(&gid(20)).is_none());
        assert!(endpoints.entities.is_empty());
        assert!(endpoints.topics.is_empty());
    }

    #[test]
    fn ordered_survivor_preserves_compatible_type_then_highest_member_gid() {
        let mut endpoints = Endpoints::default();
        let removed = entity(1, 10, true);
        for id in [24, 21, 23, 20, 22] {
            let mut candidate = entity(1, id, true);
            if id % 2 == 0 {
                candidate.type_name = "other::srv::dds_::Response_".into();
            }
            endpoints.insert(candidate);
        }
        endpoints.insert(entity(2, 30, true));
        let mut other_topic = entity(1, 31, true);
        other_topic.topic_name = "rr/otherReply".into();
        endpoints.insert(other_topic);

        // Every subset includes the empty case, an ineligible highest GID,
        // same-type preference over a higher different type, and fallback.
        for subset in 0u8..32 {
            let mut membership: HashSet<_> = (20..25)
                .filter(|id| subset & (1 << (id - 20)) != 0)
                .map(gid)
                .collect();
            membership.extend([gid(30), gid(31)]);
            let expected = endpoints
                .entities
                .values()
                .filter(|candidate| {
                    candidate.participant_key == removed.participant_key
                        && candidate.topic_name == removed.topic_name
                        && membership.contains(&candidate.key)
                })
                .max_by_key(|candidate| (candidate.type_name == removed.type_name, candidate.key))
                .map(|candidate| candidate.key);
            assert_eq!(
                endpoints
                    .replacement(&removed, &membership)
                    .map(|candidate| candidate.key),
                expected,
                "membership subset {subset}"
            );
        }
    }

    #[test]
    fn endpoint_admin_identity_follows_canonical_metadata() {
        for writer in [false, true] {
            let mut discovered = DiscoveredEntities::default();
            let mut endpoint = entity(1, 20, writer);
            let update = |discovered: &mut DiscoveredEntities, endpoint| {
                if writer {
                    discovered.add_writer(endpoint)
                } else {
                    discovered.add_reader(endpoint)
                }
            };
            assert!(update(&mut discovered, endpoint.clone()).is_empty());
            let old_admin_key = discovered.admin_space.keys().next().unwrap().clone();

            endpoint.qos.user_data = Some(vec![1, 2, 3]);
            assert!(update(&mut discovered, endpoint.clone()).is_empty());
            assert_eq!(discovered.admin_space.len(), 1);
            assert!(discovered.admin_space.contains_key(&old_admin_key));
            let metadata = if writer {
                discovered.get_writer(&endpoint.key)
            } else {
                discovered.get_reader(&endpoint.key)
            }
            .unwrap();
            assert_eq!(metadata.qos.user_data, Some(vec![1, 2, 3]));

            endpoint.participant_key = gid(2);
            endpoint.topic_name = "rr/movedReply".into();
            assert!(update(&mut discovered, endpoint).is_empty());
            assert_eq!(discovered.admin_space.len(), 1);
            assert!(!discovered.admin_space.contains_key(&old_admin_key));
            discovered.remove_participant(&gid(1));
            assert_eq!(discovered.admin_space.len(), 1);
            discovered.remove_participant(&gid(2));
            assert!(discovered.admin_space.is_empty());
            assert_eq!(discovered.readers.len(), 0);
            assert_eq!(discovered.writers.len(), 0);
        }
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
    fn incomplete_interfaces_still_replace_tracked_components() {
        for action in [false, true] {
            for client in [false, true] {
                let survivors = interface_endpoints(action, client, 0);
                let selected = interface_endpoints(action, client, 1);
                let mut endpoints = survivors.clone();
                endpoints.extend(selected.clone());
                let mut d = discover_interface(&endpoints);
                // Remove every endpoint for one component, withdrawing the
                // complete interface while retaining its other components.
                let (writer, endpoint) = &survivors[0];
                assert!(remove_endpoint(&mut d, *writer, &endpoint.key).is_empty());
                let (writer, endpoint) = &selected[0];
                assert_eq!(remove_endpoint(&mut d, *writer, &endpoint.key).len(), 1);

                // This tracked disposal has no withdrawal (already incomplete),
                // but must still find the survivor for its own component.
                let (writer, endpoint) = &selected[1];
                assert!(remove_endpoint(&mut d, *writer, &endpoint.key).is_empty());
                let (writer, endpoint) = &selected[0];
                let events = add_endpoint(&mut d, *writer, endpoint.clone());
                assert_eq!(
                    events.len(),
                    1,
                    "action={action}, client={client}: {events:?}"
                );
                let mut expected: Vec<_> = selected.iter().map(|(_, e)| e.key).collect();
                expected[1] = survivors[1].1.key;
                assert_eq!(discovered_components(&events[0]), expected);
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
        assert!(!details[withdrawal].0, "{events:?}");
        assert_eq!(details[withdrawal].1, gid(1));
        assert_eq!(details[withdrawal].2, old_name);
        assert_eq!(details[withdrawal].4, old_type);
        assert!(details[addition].0, "{events:?}");
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
        for nodes in [1, 512] {
            for (base, added) in [
                (0, 250),
                (0, 500),
                (0, 2000),
                (500, 150),
                (1850, 150),
                (1850, 300),
                (0, 12000),
                (9000, 3000),
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

#[cfg(test)]
mod survivor_shape_benchmark {
    use std::{hint::black_box, time::Instant};

    use super::*;
    use crate::ros_discovery::NodeEntitiesInfo;
    use cyclors::qos::Qos;

    const NODE: &str = "/dense_node";
    const SERVICE: &str = "/dense";
    const REPEATS: usize = 7;
    const UPDATES: usize = 65_536;

    fn participant() -> Gid {
        let mut bytes = [0x42; 16];
        bytes[12..].copy_from_slice(&[0, 0, 1, 0xc1]);
        Gid::from(bytes)
    }

    // DDS-like common participant prefix and monotonically ordered entity IDs.
    // Big-endian rank is essential: numerical descending removal must match Gid::Ord.
    fn endpoint_gid(rank: usize, writer: bool) -> Gid {
        let mut bytes = [0x42; 16];
        bytes[12..15].copy_from_slice(&((rank + 1) as u32).to_be_bytes()[1..]);
        bytes[15] = if writer { 3 } else { 4 };
        Gid::from(bytes)
    }

    fn endpoint(rank: usize, writer: bool) -> DdsEntity {
        DdsEntity {
            key: endpoint_gid(rank, writer),
            participant_key: participant(),
            topic_name: if writer {
                "rq/denseRequest"
            } else {
                "rr/denseReply"
            }
            .into(),
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

    fn discovery(entity: DdsEntity, writer: bool) -> DDSDiscoveryEvent {
        if writer {
            DDSDiscoveryEvent::DiscoveredPublication { entity }
        } else {
            DDSDiscoveryEvent::DiscoveredSubscription { entity }
        }
    }

    fn disposal(key: Gid, writer: bool) -> DDSDiscoveryEvent {
        if writer {
            DDSDiscoveryEvent::UndiscoveredPublication { key }
        } else {
            DDSDiscoveryEvent::UndiscoveredSubscription { key }
        }
    }

    fn component(entities: &ServiceCliEntities, writer: bool) -> Gid {
        if writer {
            entities.req_writer
        } else {
            entities.rep_reader
        }
    }

    fn selected(d: &DiscoveredEntities, writer: bool) -> Gid {
        component(
            &d.nodes_info[&participant()][NODE].service_cli[SERVICE].entities,
            writer,
        )
    }

    fn fixture(endpoints: usize) -> DiscoveredEntities {
        let mut d = DiscoveredEntities::default();
        assert!(d
            .apply_dds_event(DDSDiscoveryEvent::DiscoveredParticipant {
                entity: DdsParticipant {
                    key: participant(),
                    qos: Qos::default()
                },
            })
            .is_empty());
        let mut graph = ParticipantEntitiesInfo::new(participant());
        let mut node = NodeEntitiesInfo::new("/".into(), "dense_node".into());
        for rank in 0..endpoints {
            for writer in [false, true] {
                let entity = endpoint(rank, writer);
                if writer {
                    node.writer_gid_seq.insert(entity.key);
                } else {
                    node.reader_gid_seq.insert(entity.key);
                }
                assert!(d.apply_dds_event(discovery(entity, writer)).is_empty());
            }
        }
        graph.node_entities_info_seq.insert(NODE.to_owned(), node);
        assert!(matches!(
            d.update_participant_info(graph).as_slice(),
            [ROS2DiscoveryEvent::DiscoveredServiceCli(..)]
        ));
        assert_eq!(selected(&d, true), endpoint_gid(endpoints - 1, true));
        assert_eq!(selected(&d, false), endpoint_gid(endpoints - 1, false));
        assert_eq!(d.writers.len(), endpoints);
        assert_eq!(d.readers.len(), endpoints);
        d
    }

    fn canonical(d: &DiscoveredEntities, rank: usize, writer: bool) -> &DdsEntity {
        if writer {
            d.get_writer(&endpoint_gid(rank, writer)).unwrap()
        } else {
            d.get_reader(&endpoint_gid(rank, writer)).unwrap()
        }
    }

    #[test]
    #[ignore = "matched release microbenchmark; root coordinates immutable ELF comparisons"]
    fn dense_selected_disposal() {
        for endpoints in [1, 8, 64, 512, 2048] {
            for writer in [false, true] {
                for repeat in 0..REPEATS {
                    let mut d = fixture(endpoints);
                    let start = Instant::now();
                    let mut replacements = 0;
                    let mut withdrawals = 0;
                    for rank in (0..endpoints).rev() {
                        // This check stays in the measured loop for both variants:
                        // every removal must exercise the selected-survivor path.
                        let key = endpoint_gid(rank, writer);
                        assert_eq!(selected(&d, writer), key);
                        let events = black_box(d.apply_dds_event(disposal(key, writer)));
                        if rank > 0 {
                            let expected = endpoint_gid(rank - 1, writer);
                            assert!(
                                matches!(events.as_slice(), [ROS2DiscoveryEvent::DiscoveredServiceCli(p, node, service)]
                                if *p == participant() && node == NODE && service.name == SERVICE
                                && service.is_complete() && component(&service.entities, writer) == expected)
                            );
                            assert_eq!(selected(&d, writer), expected);
                            replacements += 1;
                        } else {
                            assert!(
                                matches!(events.as_slice(), [ROS2DiscoveryEvent::UndiscoveredServiceCli(p, node, service)]
                                if *p == participant() && node == NODE && service.name == SERVICE)
                            );
                            assert_eq!(selected(&d, writer), Gid::NOT_DISCOVERED);
                            withdrawals += 1;
                        }
                    }
                    let elapsed_ns = start.elapsed().as_nanos();
                    assert_eq!(replacements, endpoints - 1);
                    assert_eq!(withdrawals, 1);
                    assert_eq!(
                        if writer {
                            d.writers.len()
                        } else {
                            d.readers.len()
                        },
                        0
                    );
                    assert_eq!(
                        if writer {
                            d.readers.len()
                        } else {
                            d.writers.len()
                        },
                        endpoints
                    );
                    assert!(!d.nodes_info[&participant()][NODE].service_cli[SERVICE].is_complete());
                    println!(
                        "SURVIVOR_BENCH {}",
                        serde_json::json!({
                            "kind":"dense_selected_disposal", "manifest_dir":env!("CARGO_MANIFEST_DIR"),
                            "endpoints_per_direction":endpoints, "writer":writer, "repeat":repeat,
                            "elapsed_ns":elapsed_ns, "ns_per_removal":elapsed_ns as f64/endpoints as f64,
                            "replacements":replacements, "withdrawals":withdrawals,
                            "oracle":"actual selected GID checked before and after every measured removal",
                        })
                    );
                }
            }
        }
    }

    #[test]
    #[ignore = "matched release microbenchmark; root coordinates immutable ELF comparisons"]
    fn duplicate_metadata_updates() {
        for endpoints in [1, 64, 2048] {
            for writer in [false, true] {
                for round_robin in [false, true] {
                    if round_robin && endpoints != 2048 {
                        continue;
                    }
                    for repeat in 0..REPEATS {
                        let mut d = fixture(endpoints);
                        // Owned DDS samples are prepared outside the measured API calls.
                        let input = (0..UPDATES)
                            .map(|update| {
                                let rank = if round_robin {
                                    update % endpoints
                                } else {
                                    endpoints - 1
                                };
                                let mut entity = endpoint(rank, writer);
                                entity.qos.user_data = Some((update as u64).to_be_bytes().to_vec());
                                discovery(entity, writer)
                            })
                            .collect::<Vec<_>>();
                        let start = Instant::now();
                        let mut emitted = 0;
                        for event in input {
                            emitted += black_box(d.apply_dds_event(black_box(event))).len();
                        }
                        let elapsed_ns = start.elapsed().as_nanos();
                        assert_eq!(emitted, 0);
                        assert_eq!(d.writers.len(), endpoints);
                        assert_eq!(d.readers.len(), endpoints);
                        assert_eq!(d.admin_space.len(), 2 * endpoints + 2);
                        assert_eq!(selected(&d, true), endpoint_gid(endpoints - 1, true));
                        assert_eq!(selected(&d, false), endpoint_gid(endpoints - 1, false));
                        assert!(
                            d.nodes_info[&participant()][NODE].service_cli[SERVICE].is_complete()
                        );
                        let ranks = if round_robin {
                            0..endpoints
                        } else {
                            endpoints - 1..endpoints
                        };
                        for rank in ranks {
                            let last_update = if round_robin {
                                UPDATES - endpoints + rank
                            } else {
                                UPDATES - 1
                            };
                            assert_eq!(
                                canonical(&d, rank, writer).qos.user_data.as_deref(),
                                Some((last_update as u64).to_be_bytes().as_slice())
                            );
                        }
                        println!(
                            "SURVIVOR_BENCH {}",
                            serde_json::json!({
                                "kind":"duplicate_metadata_updates", "manifest_dir":env!("CARGO_MANIFEST_DIR"),
                                "endpoints_per_direction":endpoints, "writer":writer, "repeat":repeat,
                                "round_robin":round_robin, "updates":UPDATES, "emitted":emitted,
                                "elapsed_ns":elapsed_ns, "ns_per_update":elapsed_ns as f64/UPDATES as f64,
                                "metadata":"canonical QoS user_data checked against final per-GID update sequence",
                            })
                        );
                    }
                }
            }
        }
    }
}
