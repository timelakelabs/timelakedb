//! Cluster roles and the discovery seam (CL-1 / CL-5).
//!
//! One binary, `TIMELAKE_ROLE`: a node is `all` (the v1 default — does
//! everything, single-node, today's behaviour with the bench fixtures
//! untouched), or one specialised role in a cluster. The roles beyond `all`
//! are built and enabled one C2 phase at a time; until a role's phase lands,
//! selecting it is refused rather than half-run (`Role::implemented`).
//!
//! **Discovery is a seam, and it carries no correctness (CL-5).** Who the
//! nodes are, their roles, and their intra-cluster addresses come from a
//! [`Discovery`] backend — `static` (env/config) now, Consul at C3. The
//! standing guard: a wrong or stale membership view may waste work or
//! misroute, but it can never corrupt state, because every durable commit
//! goes through catalog CAS (C1), not through discovery. Nothing on the
//! write-durability or commit path may consult this module. It informs
//! routing and availability only.

use std::collections::BTreeMap;

/// What a node does. `All` is the whole stack in one process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    All,
    Router,
    Ingester,
    Querier,
    Compactor,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::All => "all",
            Role::Router => "router",
            Role::Ingester => "ingester",
            Role::Querier => "querier",
            Role::Compactor => "compactor",
        }
    }

    /// Parse a `TIMELAKE_ROLE` value. Case-insensitive; empty is `All` (the
    /// default a single-node deployment gets by setting nothing).
    pub fn parse(s: &str) -> Result<Role, ClusterError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "all" => Ok(Role::All),
            "router" => Ok(Role::Router),
            "ingester" => Ok(Role::Ingester),
            "querier" => Ok(Role::Querier),
            "compactor" => Ok(Role::Compactor),
            other => Err(ClusterError::UnknownRole(other.to_string())),
        }
    }

    /// Whether this role's behaviour exists yet. Roles are enabled one C2
    /// phase at a time; selecting an unbuilt role is a startup refusal, not
    /// a silent half-node. `all` (foundation) and `ingester` (CL-2, WAL
    /// replication) are built; `router`/`querier`/`compactor` are not yet.
    pub fn implemented(self) -> bool {
        matches!(self, Role::All | Role::Ingester)
    }
}

/// One node in the cluster, as discovery reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    /// Stable identity, unique in the cluster (log lines, peer selection).
    pub id: String,
    pub role: Role,
    /// `host:port` for the intra-cluster gRPC/Flight links (replication,
    /// buffer snapshots, router forwarding). Empty for a lone `all` node
    /// with no peers.
    pub address: String,
}

#[derive(Debug)]
pub enum ClusterError {
    UnknownRole(String),
    MalformedPeer(String),
}

impl std::fmt::Display for ClusterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterError::UnknownRole(s) => write!(
                f,
                "TIMELAKE_ROLE {s:?} is not one of all|router|ingester|querier|compactor"
            ),
            ClusterError::MalformedPeer(s) => write!(
                f,
                "peer {s:?} is not id=role@host:port (e.g. ing-b=ingester@ing-b:1965)"
            ),
        }
    }
}

impl std::error::Error for ClusterError {}

/// Where the cluster topology comes from. Object-safe so it can be held as
/// `Arc<dyn Discovery>` and swapped (static → Consul) by configuration.
///
/// Deliberately small: it answers "who am I" and "who else is there". It
/// does **not** decide commits, acknowledge writes, or gate durability —
/// see the module note on CL-5.
pub trait Discovery: Send + Sync {
    /// This process's own node.
    fn this_node(&self) -> &NodeInfo;

    /// Every other node the backend knows about.
    fn peers(&self) -> Vec<NodeInfo>;

    /// Peers filtered to one role — e.g. an ingester finding its pair, a
    /// router finding queriers.
    fn peers_with_role(&self, role: Role) -> Vec<NodeInfo> {
        self.peers()
            .into_iter()
            .filter(|n| n.role == role)
            .collect()
    }
}

/// Topology from env/config: no external service, no failure mode of its
/// own. The dev and C0–C2 drill backend.
pub struct StaticDiscovery {
    this: NodeInfo,
    peers: Vec<NodeInfo>,
}

impl StaticDiscovery {
    /// Construct directly (used by tests and by `from_env`).
    pub fn new(this: NodeInfo, peers: Vec<NodeInfo>) -> StaticDiscovery {
        StaticDiscovery { this, peers }
    }

    /// Build from the environment. Env reading stays at this edge so the
    /// rest is pure and testable.
    ///
    /// - `TIMELAKE_NODE_ID` — this node's id (default `node-local`).
    /// - `TIMELAKE_CLUSTER_ADDR` — this node's intra-cluster address
    ///   (default empty; a lone node needs none).
    /// - `TIMELAKE_PEERS` — comma-separated `id=role@host:port`.
    pub fn from_env(role: Role) -> Result<StaticDiscovery, ClusterError> {
        let id = std::env::var("TIMELAKE_NODE_ID").unwrap_or_else(|_| "node-local".to_string());
        let address = std::env::var("TIMELAKE_CLUSTER_ADDR").unwrap_or_default();
        let peers = match std::env::var("TIMELAKE_PEERS") {
            Ok(v) => parse_peers(&v)?,
            Err(_) => Vec::new(),
        };
        Ok(StaticDiscovery::new(NodeInfo { id, role, address }, peers))
    }
}

impl Discovery for StaticDiscovery {
    fn this_node(&self) -> &NodeInfo {
        &self.this
    }
    fn peers(&self) -> Vec<NodeInfo> {
        self.peers.clone()
    }
}

/// Parse `TIMELAKE_PEERS`: a comma-separated list of `id=role@host:port`.
/// Blank entries (a trailing comma) are skipped; a duplicate id keeps the
/// last, so a generated config that repeats a node does not fan out.
pub fn parse_peers(s: &str) -> Result<Vec<NodeInfo>, ClusterError> {
    let mut by_id: BTreeMap<String, NodeInfo> = BTreeMap::new();
    for raw in s.split(',') {
        let entry = raw.trim();
        if entry.is_empty() {
            continue;
        }
        let node = parse_peer(entry)?;
        by_id.insert(node.id.clone(), node);
    }
    Ok(by_id.into_values().collect())
}

fn parse_peer(entry: &str) -> Result<NodeInfo, ClusterError> {
    // id=role@host:port
    let (id, rest) = entry
        .split_once('=')
        .ok_or_else(|| ClusterError::MalformedPeer(entry.to_string()))?;
    let (role_s, address) = rest
        .split_once('@')
        .ok_or_else(|| ClusterError::MalformedPeer(entry.to_string()))?;
    let id = id.trim();
    let address = address.trim();
    if id.is_empty() || address.is_empty() {
        return Err(ClusterError::MalformedPeer(entry.to_string()));
    }
    Ok(NodeInfo {
        id: id.to_string(),
        role: Role::parse(role_s)?,
        address: address.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_parsing_defaults_to_all_and_rejects_typos() {
        assert_eq!(Role::parse("").unwrap(), Role::All);
        assert_eq!(Role::parse("all").unwrap(), Role::All);
        assert_eq!(Role::parse(" Ingester ").unwrap(), Role::Ingester);
        assert_eq!(Role::parse("QUERIER").unwrap(), Role::Querier);
        // A typo must not silently pick a role.
        assert!(matches!(
            Role::parse("ingster"),
            Err(ClusterError::UnknownRole(_))
        ));
    }

    #[test]
    fn implemented_roles_are_all_and_ingester() {
        assert!(Role::All.implemented());
        assert!(Role::Ingester.implemented(), "CL-2 shipped the ingester");
        for r in [Role::Router, Role::Querier, Role::Compactor] {
            assert!(!r.implemented(), "{} should not be built yet", r.as_str());
        }
    }

    #[test]
    fn peers_parse_into_addressable_nodes() {
        let peers = parse_peers("ing-a=ingester@ing-a:1965, qry-b=querier@qry-b:1966").unwrap();
        assert_eq!(peers.len(), 2);
        let a = peers.iter().find(|n| n.id == "ing-a").unwrap();
        assert_eq!(a.role, Role::Ingester);
        assert_eq!(a.address, "ing-a:1965");
    }

    #[test]
    fn blank_entries_are_skipped_and_a_repeated_id_keeps_the_last() {
        let peers = parse_peers("a=ingester@a:1, ,a=querier@a:2,").unwrap();
        assert_eq!(peers.len(), 1, "trailing comma and blank skipped");
        assert_eq!(peers[0].role, Role::Querier, "last wins on duplicate id");
    }

    #[test]
    fn malformed_peers_are_a_loud_error_not_a_silent_drop() {
        assert!(parse_peers("no-at-sign").is_err());
        assert!(parse_peers("id=ingester@").is_err(), "empty address");
        assert!(parse_peers("=ingester@h:1").is_err(), "empty id");
        assert!(parse_peers("id=notarole@h:1").is_err(), "bad role");
    }

    #[test]
    fn discovery_reports_self_and_filters_peers_by_role() {
        let this = NodeInfo {
            id: "ing-a".into(),
            role: Role::Ingester,
            address: "ing-a:1965".into(),
        };
        let peers = parse_peers("ing-b=ingester@ing-b:1965, qry=querier@qry:1966").unwrap();
        let d = StaticDiscovery::new(this, peers);
        assert_eq!(d.this_node().id, "ing-a");
        let ingesters = d.peers_with_role(Role::Ingester);
        assert_eq!(ingesters.len(), 1);
        assert_eq!(ingesters[0].id, "ing-b", "the pair peer, not self");
        assert_eq!(d.peers_with_role(Role::Querier).len(), 1);
        assert!(d.peers_with_role(Role::Router).is_empty());
    }

    #[test]
    fn a_lone_node_has_no_peers() {
        let d = StaticDiscovery::new(
            NodeInfo {
                id: "node-local".into(),
                role: Role::All,
                address: String::new(),
            },
            Vec::new(),
        );
        assert!(d.peers().is_empty());
        assert!(d.peers_with_role(Role::Ingester).is_empty());
    }
}
