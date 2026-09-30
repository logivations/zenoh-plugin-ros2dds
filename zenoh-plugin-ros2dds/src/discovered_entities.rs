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
    pub fn get_writer(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.writers.get(gid)
    }

    #[inline]
    fn add_writer(&mut self, entity: DdsEntity) -> Vec<ROS2DiscoveryEvent> {
        self.admin_space.insert(
            keformat!(
                ke_admin_writer::formatter(),
                pgid = entity.participant_key,
                wgid = entity.key,
                topic = &entity.topic_name
            )
            .unwrap(),
            EntityRef::Writer(entity.key),
        );
        let mut events = Vec::new();
        if let Some(nodes) = self.nodes_info.get_mut(&entity.participant_key) {
            for node in nodes.values_mut() {
                if let Some(index) = node
                    .undiscovered_writer
                    .iter()
                    .position(|gid| *gid == entity.key)
                {
                    node.undiscovered_writer.remove(index);
                    events.extend(node.update_with_writer(&entity));
                }
            }
        }
        self.writers.insert(entity.key, entity);
        events
    }

    #[inline]
    fn remove_writer(&mut self, gid: &Gid) -> Vec<ROS2DiscoveryEvent> {
        let Some(entity) = self.writers.remove(gid) else {
            return Vec::new();
        };
        self.admin_space.remove(
            &keformat!(
                ke_admin_writer::formatter(),
                pgid = entity.participant_key,
                wgid = entity.key,
                topic = &entity.topic_name
            )
            .unwrap(),
        );
        let mut events = Vec::new();
        if let (Some(graph), Some(nodes)) = (
            self.ros_participant_info.get(&entity.participant_key),
            self.nodes_info.get_mut(&entity.participant_key),
        ) {
            // A node may own several endpoints for the same interface. Select a
            // surviving counterpart BEFORE removing this one, so losing one of
            // several service clients cannot retire their shared route.
            for (name, ros_node) in &graph.node_entities_info_seq {
                let membership = &ros_node.writer_gid_seq;
                if membership.contains(gid) {
                    let Some(node) = nodes.get_mut(name) else {
                        continue;
                    };
                    if let Some(replacement) = membership
                        .iter()
                        .filter_map(|candidate| self.writers.get(candidate))
                        .filter(|candidate| {
                            candidate.participant_key == entity.participant_key
                                && candidate.topic_name == entity.topic_name
                        })
                        .max_by_key(|candidate| candidate.key)
                    {
                        events.extend(node.update_with_writer(replacement));
                    }
                    events.extend(node.remove_writer(gid));
                    node.undiscovered_writer.push(*gid);
                }
            }
        }
        events
    }

    #[inline]
    pub fn get_reader(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.readers.get(gid)
    }

    #[inline]
    fn add_reader(&mut self, entity: DdsEntity) -> Vec<ROS2DiscoveryEvent> {
        self.admin_space.insert(
            keformat!(
                ke_admin_reader::formatter(),
                pgid = entity.participant_key,
                wgid = entity.key,
                topic = &entity.topic_name
            )
            .unwrap(),
            EntityRef::Reader(entity.key),
        );
        let mut events = Vec::new();
        if let Some(nodes) = self.nodes_info.get_mut(&entity.participant_key) {
            for node in nodes.values_mut() {
                if let Some(index) = node
                    .undiscovered_reader
                    .iter()
                    .position(|gid| *gid == entity.key)
                {
                    node.undiscovered_reader.remove(index);
                    events.extend(node.update_with_reader(&entity));
                }
            }
        }
        self.readers.insert(entity.key, entity);
        events
    }

    #[inline]
    fn remove_reader(&mut self, gid: &Gid) -> Vec<ROS2DiscoveryEvent> {
        let Some(entity) = self.readers.remove(gid) else {
            return Vec::new();
        };
        self.admin_space.remove(
            &keformat!(
                ke_admin_reader::formatter(),
                pgid = entity.participant_key,
                wgid = entity.key,
                topic = &entity.topic_name
            )
            .unwrap(),
        );
        let mut events = Vec::new();
        if let (Some(graph), Some(nodes)) = (
            self.ros_participant_info.get(&entity.participant_key),
            self.nodes_info.get_mut(&entity.participant_key),
        ) {
            // A node may own several endpoints for the same interface. Select a
            // surviving counterpart BEFORE removing this one, so losing one of
            // several service clients cannot retire their shared route.
            for (name, ros_node) in &graph.node_entities_info_seq {
                let membership = &ros_node.reader_gid_seq;
                if membership.contains(gid) {
                    let Some(node) = nodes.get_mut(name) else {
                        continue;
                    };
                    if let Some(replacement) = membership
                        .iter()
                        .filter_map(|candidate| self.readers.get(candidate))
                        .filter(|candidate| {
                            candidate.participant_key == entity.participant_key
                                && candidate.topic_name == entity.topic_name
                        })
                        .max_by_key(|candidate| candidate.key)
                    {
                        events.extend(node.update_with_reader(replacement));
                    }
                    events.extend(node.remove_reader(gid));
                    node.undiscovered_reader.push(*gid);
                }
            }
        }
        events
    }

    /// Update only the affected interface, as upstream does. ROS graph
    /// snapshots reconcile membership; DDS events must not reconstruct
    /// unrelated routes, even when the events arrive one at a time.
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
    /// the difference. Never incrementally delete a whole pair from one endpoint
    /// event, and never carry endpoints absent from the current ROS snapshot.
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
        events
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
                // the still-present lower GIDs must keep the interface complete.
                for (writer, endpoint) in replacements {
                    let events = remove_endpoint(&mut d, writer, &endpoint.key);
                    assert!(
                        events.iter().all(|event| matches!(
                            event,
                            ROS2DiscoveryEvent::DiscoveredServiceSrv(..)
                                | ROS2DiscoveryEvent::DiscoveredServiceCli(..)
                                | ROS2DiscoveryEvent::DiscoveredActionSrv(..)
                                | ROS2DiscoveryEvent::DiscoveredActionCli(..)
                        )),
                        "unexpected withdrawal: {events:?}"
                    );
                }
                for (writer, endpoint) in interface_endpoints(action, client, 0) {
                    remove_endpoint(&mut d, writer, &endpoint.key);
                }
                let node = &d.nodes_info[&gid(1)]["/camera"];
                assert!(node.service_srv.values().all(|s| !s.is_complete()));
                assert!(node.service_cli.values().all(|s| !s.is_complete()));
                assert!(node.action_srv.values().all(|s| !s.is_complete()));
                assert!(node.action_cli.values().all(|s| !s.is_complete()));
            }
        }
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
