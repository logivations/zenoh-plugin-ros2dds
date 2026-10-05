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
    ros_discovery::{NodeEntitiesInfo, ParticipantEntitiesInfo},
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
    pub(crate) fn nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes_info.values().flat_map(|nodes| nodes.values())
    }

    #[inline]
    pub fn add_participant(&mut self, participant: DdsParticipant) {
        self.admin_space.insert(
            keformat!(ke_admin_participant::formatter(), pgid = participant.key).unwrap(),
            EntityRef::Participant(participant.key),
        );
        self.participants.insert(participant.key, participant);
    }

    #[inline]
    pub fn remove_participant(&mut self, gid: &Gid) -> Vec<ROS2DiscoveryEvent> {
        let mut events: Vec<ROS2DiscoveryEvent> = Vec::new();
        // Remove Participant from participants list and from admin_space
        self.participants.remove(gid);
        self.admin_space
            .remove(&keformat!(ke_admin_participant::formatter(), pgid = gid).unwrap());
        // Remove associated NodeInfos
        if let Some(nodes) = self.nodes_info.remove(gid) {
            for (name, mut node) in nodes {
                tracing::info!("Undiscovered ROS Node {}", name);
                self.admin_space.remove(
                    &keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr(),)
                        .unwrap(),
                );
                // return undiscovery events for this node
                events.append(&mut node.remove_all_entities());
            }
        }
        events
    }

    #[inline]
    pub fn add_writer(&mut self, writer: DdsEntity) -> Option<ROS2DiscoveryEvent> {
        // insert in admin space
        self.admin_space.insert(
            keformat!(
                ke_admin_writer::formatter(),
                pgid = writer.participant_key,
                wgid = writer.key,
                topic = &writer.topic_name,
            )
            .unwrap(),
            EntityRef::Writer(writer.key),
        );

        // Check if this Writer is present in some NodeInfo.undiscovered_writer list
        let mut event: Option<ROS2DiscoveryEvent> = None;
        for nodes_map in self.nodes_info.values_mut() {
            for node in nodes_map.values_mut() {
                if let Some(i) = node
                    .undiscovered_writer
                    .iter()
                    .position(|gid| gid == &writer.key)
                {
                    // update the NodeInfo with this Writer's info
                    node.undiscovered_writer.remove(i);
                    event = node.update_with_writer(&writer);
                    break;
                }
            }
            if event.is_some() {
                break;
            }
        }

        // insert in Writers list
        self.writers.insert(writer.key, writer);
        event
    }

    #[inline]
    pub fn get_writer(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.writers.get(gid)
    }

    #[inline]
    pub fn remove_writer(&mut self, gid: &Gid) -> Option<ROS2DiscoveryEvent> {
        if let Some(writer) = self.writers.remove(gid) {
            self.admin_space.remove(
                &keformat!(
                    ke_admin_writer::formatter(),
                    pgid = writer.participant_key,
                    wgid = writer.key,
                    topic = &writer.topic_name,
                )
                .unwrap(),
            );

            // Remove the Writer from any NodeInfo that might use it, possibly leading to a UndiscoveredX event
            for nodes_map in self.nodes_info.values_mut() {
                for node in nodes_map.values_mut() {
                    if let Some(e) = node.remove_writer(gid) {
                        // A Reader can be used by only 1 Node, no need to go on with loops
                        return Some(e);
                    }
                }
            }
        }
        None
    }

    #[inline]
    pub fn add_reader(&mut self, reader: DdsEntity) -> Option<ROS2DiscoveryEvent> {
        // insert in admin space
        self.admin_space.insert(
            keformat!(
                ke_admin_reader::formatter(),
                pgid = reader.participant_key,
                wgid = reader.key,
                topic = &reader.topic_name,
            )
            .unwrap(),
            EntityRef::Reader(reader.key),
        );

        // Check if this Reader is present in some NodeInfo.undiscovered_reader list
        let mut event = None;
        for nodes_map in self.nodes_info.values_mut() {
            for node in nodes_map.values_mut() {
                if let Some(i) = node
                    .undiscovered_reader
                    .iter()
                    .position(|gid| gid == &reader.key)
                {
                    // update the NodeInfo with this Reader's info
                    node.undiscovered_reader.remove(i);
                    event = node.update_with_reader(&reader);
                    break;
                }
            }
            if event.is_some() {
                break;
            }
        }

        // insert in Readers list
        self.readers.insert(reader.key, reader);
        event
    }

    #[inline]
    pub fn get_reader(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.readers.get(gid)
    }

    #[inline]
    pub fn remove_reader(&mut self, gid: &Gid) -> Option<ROS2DiscoveryEvent> {
        if let Some(reader) = self.readers.remove(gid) {
            self.admin_space.remove(
                &keformat!(
                    ke_admin_reader::formatter(),
                    pgid = reader.participant_key,
                    wgid = reader.key,
                    topic = &reader.topic_name,
                )
                .unwrap(),
            );

            // Remove the Reader from any NodeInfo that might use it, possibly leading to a UndiscoveredX event
            for nodes_map in self.nodes_info.values_mut() {
                for node in nodes_map.values_mut() {
                    if let Some(e) = node.remove_reader(gid) {
                        // A Reader can be used by only 1 Node, no need to go on with loops
                        return Some(e);
                    }
                }
            }
        }
        None
    }

    pub fn update_participant_info(
        &mut self,
        ros_info: ParticipantEntitiesInfo,
    ) -> Vec<ROS2DiscoveryEvent> {
        let mut events: Vec<ROS2DiscoveryEvent> = Vec::new();
        let Self {
            writers,
            readers,
            nodes_info,
            admin_space,
            ..
        } = self;
        let nodes_map = nodes_info.entry(ros_info.gid).or_insert_with(HashMap::new);

        // Remove nodes that are no longer present in ParticipantEntitiesInfo
        nodes_map.retain(|name, node| {
            if !ros_info.node_entities_info_seq.contains_key(name) {
                tracing::info!("Undiscovered ROS Node {}", name);
                admin_space.remove(
                    &keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr(),)
                        .unwrap(),
                );
                // return undiscovery events for this node
                events.append(&mut node.remove_all_entities());
                false
            } else {
                true
            }
        });

        // For each declared node in this ros_node_info
        for (name, ros_node_info) in &ros_info.node_entities_info_seq {
            // If node was not yet discovered, add a new NodeInfo
            if !nodes_map.contains_key(name) {
                tracing::info!("Discovered ROS Node {}", name);
                match NodeInfo::create(
                    ros_node_info.node_namespace.clone(),
                    ros_node_info.node_name.clone(),
                    ros_info.gid,
                ) {
                    Ok(node) => {
                        self.admin_space.insert(
                            keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr(),)
                                .unwrap(),
                            EntityRef::Node(ros_info.gid, node.fullname().to_string()),
                        );
                        nodes_map.insert(node.fullname().to_string(), node);
                    }
                    Err(e) => {
                        tracing::warn!("ROS Node has incompatible name: {e}");
                        break;
                    }
                }
            };

            // Update NodeInfo, adding resulting events to the list
            let node = nodes_map.get_mut(name).unwrap();
            events.append(&mut Self::update_node_info(
                node,
                ros_node_info,
                readers,
                writers,
            ));
        }

        // Save ParticipantEntitiesInfo
        self.ros_participant_info.insert(ros_info.gid, ros_info);
        events
    }

    pub fn update_node_info(
        node: &mut NodeInfo,
        ros_node_info: &NodeEntitiesInfo,
        readers: &mut HashMap<Gid, DdsEntity>,
        writers: &mut HashMap<Gid, DdsEntity>,
    ) -> Vec<ROS2DiscoveryEvent> {
        let mut events = Vec::new();
        // For each declared Reader
        for rgid in &ros_node_info.reader_gid_seq {
            if let Some(entity) = readers.get(rgid) {
                tracing::trace!(
                    "ROS Node {ros_node_info} declares a Reader on {}",
                    entity.topic_name
                );
                if let Some(e) = node.update_with_reader(entity) {
                    tracing::debug!(
                        "ROS Node {ros_node_info} declares a new Reader on {}",
                        entity.topic_name
                    );
                    events.push(e)
                };
            } else if !node.undiscovered_reader.contains(rgid) {
                tracing::debug!(
                    "ROS Node {ros_node_info} declares a not yet discovered DDS Reader: {rgid}"
                );
                node.undiscovered_reader.push(*rgid);
            }
        }
        // For each declared Writer
        for wgid in &ros_node_info.writer_gid_seq {
            if let Some(entity) = writers.get(wgid) {
                tracing::trace!(
                    "ROS Node {ros_node_info} declares Writer on {}",
                    entity.topic_name
                );
                if let Some(e) = node.update_with_writer(entity) {
                    tracing::debug!(
                        "ROS Node {ros_node_info} declares a new Writer on {}",
                        entity.topic_name
                    );
                    events.push(e)
                };
            } else if !node.undiscovered_writer.contains(wgid) {
                tracing::debug!(
                    "ROS Node {ros_node_info} declares a not yet discovered DDS Writer: {wgid}"
                );
                node.undiscovered_writer.push(*wgid);
            }
        }
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

// TEST-ONLY (RTDTK-1026 picks qualification, gate G6): benchmark for the
// undiscovered-gid staging cost in update_node_info. Not for upstreaming.
#[cfg(test)]
mod bench_undiscovered_staging {
    use std::{collections::HashMap, time::Instant};

    use super::DiscoveredEntities;
    use crate::{
        dds_discovery::DdsEntity, gid::Gid, node_info::NodeInfo, ros_discovery::NodeEntitiesInfo,
    };

    fn make_gid(i: u64, salt: u8) -> Gid {
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&i.to_le_bytes());
        b[8] = salt;
        b[15] = 1;
        Gid::from(b)
    }

    /// Thread CPU time in ms from /proc/thread-self/stat (utime+stime,
    /// USER_HZ=100 -> 10 ms resolution).
    fn thread_cpu_ms() -> f64 {
        let stat = match std::fs::read_to_string("/proc/thread-self/stat") {
            Ok(s) => s,
            Err(_) => return f64::NAN,
        };
        let after = match stat.rsplit_once(')') {
            Some((_, rest)) => rest,
            None => return f64::NAN,
        };
        let fields: Vec<&str> = after.split_whitespace().collect();
        // after ')' : field 0 = state, utime = overall field 14 -> index 11
        let utime: u64 = fields.get(11).and_then(|v| v.parse().ok()).unwrap_or(0);
        let stime: u64 = fields.get(12).and_then(|v| v.parse().ok()).unwrap_or(0);
        ((utime + stime) as f64) * 10.0
    }

    fn run_case(n: usize, rounds: usize) {
        let mut info = NodeEntitiesInfo::new("/".to_string(), format!("bench_{n}"));
        for i in 0..n {
            info.reader_gid_seq.insert(make_gid(i as u64, 2));
        }
        let mut node =
            NodeInfo::create("/".to_string(), format!("bench_{n}"), make_gid(u64::MAX, 9))
                .expect("NodeInfo::create");
        let mut readers: HashMap<Gid, DdsEntity> = HashMap::new();
        let mut writers: HashMap<Gid, DdsEntity> = HashMap::new();
        for round in 1..=rounds {
            let cpu0 = thread_cpu_ms();
            let t0 = Instant::now();
            let events =
                DiscoveredEntities::update_node_info(&mut node, &info, &mut readers, &mut writers);
            let wall_ms = t0.elapsed().as_secs_f64() * 1000.0;
            let cpu_ms = thread_cpu_ms() - cpu0;
            println!(
                "G6BENCH n={n} round={round} wall_ms={wall_ms:.3} cpu_ms={cpu_ms:.1} \
                 undiscovered_len={} events={}",
                node.undiscovered_reader.len(),
                events.len()
            );
        }
    }

    #[test]
    #[ignore = "G6 benchmark, run explicitly"]
    fn bench_undiscovered_staging_cost() {
        for n in [1000usize, 6000, 24000] {
            run_case(n, 10);
        }
    }
}

// Regression tests for the #713 pick: gids declared by a graph announcement
// but not yet discovered on DDS are staged exactly once per node.
#[cfg(test)]
mod undiscovered_staging_tests {
    use std::collections::HashMap;

    use cyclors::qos::Qos;

    use super::DiscoveredEntities;
    use crate::{
        dds_discovery::DdsEntity, gid::Gid, node_info::NodeInfo, ros_discovery::NodeEntitiesInfo,
    };

    fn test_gid(i: u64, salt: u8) -> Gid {
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&i.to_le_bytes());
        b[8] = salt;
        b[15] = 1;
        Gid::from(b)
    }

    #[test]
    fn repeated_node_info_does_not_duplicate_staged_gids() {
        const N: u64 = 10;
        let mut info = NodeEntitiesInfo::new("/".to_string(), "cam".to_string());
        for i in 0..N {
            info.reader_gid_seq.insert(test_gid(i, 2));
            info.writer_gid_seq.insert(test_gid(i, 3));
        }
        let mut node =
            NodeInfo::create("/".to_string(), "cam".to_string(), test_gid(999, 9)).unwrap();
        let mut readers: HashMap<Gid, DdsEntity> = HashMap::new();
        let mut writers: HashMap<Gid, DdsEntity> = HashMap::new();

        // the same announcement processed twice must not duplicate the
        // staged undiscovered gids (readers and writers)
        for round in 1..=2 {
            let events =
                DiscoveredEntities::update_node_info(&mut node, &info, &mut readers, &mut writers);
            assert!(events.is_empty(), "round {round}: no endpoint discovered");
            assert_eq!(node.undiscovered_reader.len(), N as usize, "round {round}");
            assert_eq!(node.undiscovered_writer.len(), N as usize, "round {round}");
        }
    }

    #[test]
    fn discovered_gid_leaves_staging() {
        let participant = test_gid(77, 9);
        let reader_gid = test_gid(1, 2);

        // stage one undiscovered reader gid via a graph announcement
        let mut info = NodeEntitiesInfo::new("/".to_string(), "cam".to_string());
        info.reader_gid_seq.insert(reader_gid);
        let mut node = NodeInfo::create("/".to_string(), "cam".to_string(), participant).unwrap();
        let mut readers: HashMap<Gid, DdsEntity> = HashMap::new();
        let mut writers: HashMap<Gid, DdsEntity> = HashMap::new();
        DiscoveredEntities::update_node_info(&mut node, &info, &mut readers, &mut writers);
        assert_eq!(node.undiscovered_reader, vec![reader_gid]);

        // when the DDS Reader with that gid is discovered, the gid must
        // leave the staging list and produce the discovery event
        let fullname = node.fullname().to_string();
        let mut entities = DiscoveredEntities::default();
        entities
            .nodes_info
            .insert(participant, HashMap::from([(fullname.clone(), node)]));

        let event = entities.add_reader(DdsEntity {
            key: reader_gid,
            participant_key: participant,
            topic_name: "rt/detections".to_string(),
            type_name: "std_msgs::msg::dds_::String_".to_string(),
            _type_info: None,
            keyless: true,
            qos: Qos::default(),
        });
        assert!(event.is_some(), "a complete Subscriber must be discovered");

        let node = &entities.nodes_info[&participant][&fullname];
        assert!(node.undiscovered_reader.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn only_allowed_complete_live_endpoints_require_a_local_route() {
        use std::sync::{Arc, RwLock};

        use crate::{
            config::Config,
            node_info::{ActionCli, ActionSrv, MsgPub, MsgSub, ServiceCli, ServiceSrv},
            ros_discovery::RosDiscoveryInfoMgr,
            routes_mgr::RoutesMgr,
        };

        // Use real route ownership, DDS and Zenoh resources. Only the native
        // discovery input is constructed, as when a route-creation event is lost.
        struct Participant(cyclors::dds_entity_t);
        impl Drop for Participant {
            fn drop(&mut self) {
                unsafe { cyclors::dds_delete(self.0) };
            }
        }
        let participant = Participant(unsafe {
            cyclors::dds_create_participant(232, std::ptr::null(), std::ptr::null())
        });
        assert!(participant.0 > 0);
        let mut zconfig = zenoh::Config::default();
        zconfig
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();
        zconfig.insert_json5("listen/endpoints", "[]").unwrap();
        let session = Arc::new(zenoh::open(zconfig).await.unwrap());
        let config: Config = serde_json::from_value(serde_json::json!({
            "namespace": "/robot",
            "allow": {
                "publishers": ["/wanted"], "subscribers": ["/wanted"],
                "service_servers": ["/wanted"], "service_clients": ["/wanted"],
                "action_servers": ["/wanted"], "action_clients": ["/wanted"]
            }
        }))
        .unwrap();
        let discovered = Arc::new(RwLock::new(DiscoveredEntities::default()));
        let manager = RoutesMgr::new(
            Arc::new(config),
            session,
            participant.0,
            discovered.clone(),
            Arc::new(RosDiscoveryInfoMgr::new(participant.0, "/", "health_test").unwrap()),
            "@/health-test/ros2".try_into().unwrap(),
        );
        let gid = test_gid(81, 9);
        let mut node = NodeInfo::create("/".into(), "source".into(), gid).unwrap();
        let id = node.id.clone();
        for name in ["/wanted", "/denied"] {
            node.msg_pub.insert(
                name.into(),
                MsgPub::create(name.into(), "T".into(), gid).unwrap(),
            );
            node.msg_sub.insert(
                name.into(),
                MsgSub::create(name.into(), "T".into(), gid).unwrap(),
            );
            node.service_srv.insert(
                name.into(),
                ServiceSrv::create(name.into(), "T".into()).unwrap(),
            );
            node.service_cli.insert(
                name.into(),
                ServiceCli::create(name.into(), "T".into()).unwrap(),
            );
            node.action_srv.insert(
                name.into(),
                ActionSrv::create(name.into(), "T".into()).unwrap(),
            );
            node.action_cli.insert(
                name.into(),
                ActionCli::create(name.into(), "T".into()).unwrap(),
            );
        }
        discovered
            .write()
            .unwrap()
            .nodes_info
            .insert(gid, HashMap::from([("/source".into(), node)]));
        let missing = serde_json::to_value(manager.missing_local_routes()).unwrap();
        assert_eq!(
            missing.as_array().unwrap().len(),
            2,
            "incomplete services/actions and denied topics are not required"
        );
        for row in missing.as_array().unwrap() {
            assert_eq!(row["node"], id);
            assert_eq!(row["zenoh_key_expr"], "robot/wanted");
            assert_eq!(row["ros2_name"], "/wanted");
            assert_eq!(row["ros2_type"], "T");
        }

        {
            let mut graph = discovered.write().unwrap();
            let node = graph
                .nodes_info
                .get_mut(&gid)
                .unwrap()
                .get_mut("/source")
                .unwrap();
            for srv in node.service_srv.values_mut() {
                srv.entities.req_reader = gid;
                srv.entities.rep_writer = gid;
            }
            for cli in node.service_cli.values_mut() {
                cli.entities.req_writer = gid;
                cli.entities.rep_reader = gid;
            }
            for srv in node.action_srv.values_mut() {
                srv.entities.send_goal = node.service_srv["/wanted"].entities;
                srv.entities.cancel_goal = srv.entities.send_goal;
                srv.entities.get_result = srv.entities.send_goal;
                srv.entities.status_writer = gid;
                srv.entities.feedback_writer = gid;
            }
            for cli in node.action_cli.values_mut() {
                cli.entities.send_goal = node.service_cli["/wanted"].entities;
                cli.entities.cancel_goal = cli.entities.send_goal;
                cli.entities.get_result = cli.entities.send_goal;
                cli.entities.status_reader = gid;
                cli.entities.feedback_reader = gid;
            }
        }
        let missing = serde_json::to_value(manager.missing_local_routes()).unwrap();
        let routes: std::collections::HashSet<_> = missing
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["route"].as_str().unwrap())
            .collect();
        assert_eq!(
            routes,
            std::collections::HashSet::from([
                "topic/pub/robot/wanted",
                "topic/sub/robot/wanted",
                "service/srv/robot/wanted",
                "service/cli/robot/wanted",
                "action/srv/robot/wanted",
                "action/cli/robot/wanted",
            ])
        );
        discovered.write().unwrap().remove_participant(&gid);
        assert!(
            manager.missing_local_routes().is_empty(),
            "an application which left the native discovery graph is not a bridge fault"
        );
    }
}
