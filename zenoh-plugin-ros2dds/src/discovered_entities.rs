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
    dds_discovery::{DdsEntity, DdsParticipant},
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
    pub fn add_writer(&mut self, writer: DdsEntity) -> Vec<ROS2DiscoveryEvent> {
        let participant = writer.participant_key;
        self.admin_space.insert(
            keformat!(
                ke_admin_writer::formatter(),
                pgid = participant,
                wgid = writer.key,
                topic = &writer.topic_name
            )
            .unwrap(),
            EntityRef::Writer(writer.key),
        );
        self.writers.insert(writer.key, writer);
        self.reconcile_participant(participant)
    }

    #[inline]
    pub fn get_writer(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.writers.get(gid)
    }

    #[inline]
    pub fn remove_writer(&mut self, gid: &Gid) -> Vec<ROS2DiscoveryEvent> {
        match self.writers.remove(gid) {
            Some(writer) => {
                self.admin_space.remove(
                    &keformat!(
                        ke_admin_writer::formatter(),
                        pgid = writer.participant_key,
                        wgid = writer.key,
                        topic = &writer.topic_name
                    )
                    .unwrap(),
                );
                self.reconcile_participant(writer.participant_key)
            }
            None => Vec::new(),
        }
    }

    #[inline]
    pub fn add_reader(&mut self, reader: DdsEntity) -> Vec<ROS2DiscoveryEvent> {
        let participant = reader.participant_key;
        self.admin_space.insert(
            keformat!(
                ke_admin_reader::formatter(),
                pgid = participant,
                wgid = reader.key,
                topic = &reader.topic_name
            )
            .unwrap(),
            EntityRef::Reader(reader.key),
        );
        self.readers.insert(reader.key, reader);
        self.reconcile_participant(participant)
    }

    #[inline]
    pub fn get_reader(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.readers.get(gid)
    }

    #[inline]
    pub fn remove_reader(&mut self, gid: &Gid) -> Vec<ROS2DiscoveryEvent> {
        match self.readers.remove(gid) {
            Some(reader) => {
                self.admin_space.remove(
                    &keformat!(
                        ke_admin_reader::formatter(),
                        pgid = reader.participant_key,
                        wgid = reader.key,
                        topic = &reader.topic_name
                    )
                    .unwrap(),
                );
                self.reconcile_participant(reader.participant_key)
            }
            None => Vec::new(),
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
