//! Consul-backed discovery (CL-5 v2, #71) — the live backend for
//! [`timelake_cluster::Discovery`].
//!
//! Static discovery (`TIMELAKE_PEERS`) is fixed at boot: a membership change
//! is a restart. `ConsulDiscovery` registers this node as a Consul service and
//! keeps a background-refreshed snapshot of the healthy members, so a node can
//! join or leave a live cluster without hand-editing every peer's env.
//!
//! **CL-5 holds exactly as it does for the static backend.** Discovery informs
//! routing and availability only; nothing on the write/commit path reads it,
//! and every durable commit goes through catalog CAS. A stale or lying Consul
//! view can misroute or waste work — never corrupt. Two properties keep that
//! true here:
//!
//! - [`ConsulDiscovery::peers`] is a **lock-free read of the last snapshot**,
//!   never a Consul round-trip — a hot path must not carry network latency.
//! - When Consul is unreachable the snapshot is **held at last-known-good** and
//!   an alarm is raised; it never blocks, panics, or empties the membership out
//!   from under the cluster. Same posture as a down CL-2 peer (PR-7): keep
//!   serving on what is known, alarm, recover.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use timelake_cluster::{Discovery, NodeInfo, Role};

/// The Consul service name every node registers under.
const SERVICE: &str = "timelakedb";
/// TTL for the self-passed health check. The refresh loop passes it every
/// `interval` (which must be shorter), so a node that dies stops passing and
/// Consul drops it from the healthy set — that is how a *leave* is detected
/// without Consul needing to reach the node (which may be behind TLS).
const CHECK_TTL_SECS: u64 = 30;

/// Consul discovery counters, surfaced by the owner on `/metrics`.
#[derive(Default)]
pub struct ConsulStats {
    /// Catalog refreshes that succeeded.
    pub refreshes: AtomicU64,
    /// Refreshes that failed (Consul unreachable). The last-good snapshot is
    /// kept, so this is an availability signal, not data loss.
    pub refresh_failures: AtomicU64,
    /// 1 while the last refresh failed — the gauge an operator alarms on.
    pub degraded: AtomicBool,
}

/// A Consul-backed [`Discovery`]. Construct with [`ConsulDiscovery::start`].
pub struct ConsulDiscovery {
    this: NodeInfo,
    /// The last healthy membership snapshot (excluding self). Read lock-free by
    /// `peers()`, written by the background refresh task.
    members: Arc<ArcSwap<Vec<NodeInfo>>>,
    stats: Arc<ConsulStats>,
}

impl ConsulDiscovery {
    /// Register `this` with Consul at `consul_base` (e.g.
    /// `http://consul:8500`), do one refresh so `peers()` is populated before
    /// the caller wires it in, then refresh every `interval` in the background.
    ///
    /// Returns even if Consul is initially unreachable: a cluster that starts
    /// before Consul is up must degrade and recover, not deadlock. The first
    /// refresh's outcome is visible on [`ConsulStats`].
    pub async fn start(
        this: NodeInfo,
        consul_base: &str,
        interval: Duration,
    ) -> Arc<ConsulDiscovery> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("consul http client");
        let base = consul_base.trim_end_matches('/').to_string();
        let members = Arc::new(ArcSwap::from_pointee(Vec::new()));
        let stats = Arc::new(ConsulStats::default());

        // First pass synchronously, so peers() is not empty for the window
        // between wiring and the first tick — best-effort, degrades on failure.
        refresh(&client, &base, &this, &members, &stats).await;

        let bg = (
            client,
            base,
            this.clone(),
            Arc::clone(&members),
            Arc::clone(&stats),
        );
        tokio::spawn(async move {
            let (client, base, this, members, stats) = bg;
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                refresh(&client, &base, &this, &members, &stats).await;
            }
        });

        Arc::new(ConsulDiscovery {
            this,
            members,
            stats,
        })
    }

    pub fn stats(&self) -> &ConsulStats {
        &self.stats
    }
}

impl Discovery for ConsulDiscovery {
    fn this_node(&self) -> &NodeInfo {
        &self.this
    }

    fn peers(&self) -> Vec<NodeInfo> {
        // A lock-free read of the last snapshot — never an HTTP call.
        self.members.load().as_ref().clone()
    }
}

/// One refresh cycle: re-register (self-heal if Consul restarted and forgot
/// us), pass the TTL check, and pull the healthy members. On any failure the
/// last-good snapshot is kept and the degraded alarm is raised once.
async fn refresh(
    client: &reqwest::Client,
    base: &str,
    this: &NodeInfo,
    members: &ArcSwap<Vec<NodeInfo>>,
    stats: &ConsulStats,
) {
    match refresh_once(client, base, this).await {
        Ok(peers) => {
            members.store(Arc::new(peers));
            stats.refreshes.fetch_add(1, Ordering::Relaxed);
            if stats.degraded.swap(false, Ordering::Relaxed) {
                tracing::info!("consul discovery recovered — membership refreshed");
            }
        }
        Err(e) => {
            stats.refresh_failures.fetch_add(1, Ordering::Relaxed);
            if !stats.degraded.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    alarm = "CONSUL_DISCOVERY_DEGRADED",
                    error = %e,
                    "Consul unreachable — serving on the last-known-good membership; \
                     routing may be stale but no write path consults discovery (CL-5)"
                );
            }
        }
    }
}

async fn refresh_once(
    client: &reqwest::Client,
    base: &str,
    this: &NodeInfo,
) -> Result<Vec<NodeInfo>, reqwest::Error> {
    register(client, base, this).await?;
    pass_check(client, base, this).await?;
    fetch_members(client, base, &this.id).await
}

/// Register (or re-register — the PUT is idempotent by service ID) this node,
/// carrying its role and addresses in service Meta so a peer can rebuild the
/// exact `NodeInfo` a static config would have produced.
async fn register(
    client: &reqwest::Client,
    base: &str,
    this: &NodeInfo,
) -> Result<(), reqwest::Error> {
    let (host, port) = split_host_port(&this.address);
    let payload = serde_json::json!({
        "ID": this.id,
        "Name": SERVICE,
        "Address": host,
        "Port": port,
        "Meta": {
            "role": this.role.as_str(),
            "cluster_addr": this.address,
            "data_addr": this.data_address,
        },
        // A TTL check we pass ourselves: no need for Consul to reach the node
        // (which may be behind required mTLS). A node that dies stops passing
        // and Consul deregisters it, which is how a leave is detected.
        "Check": {
            "CheckID": check_id(&this.id),
            "TTL": format!("{CHECK_TTL_SECS}s"),
            "DeregisterCriticalServiceAfter": "1m",
        },
    });
    client
        .put(format!("{base}/v1/agent/service/register"))
        .body(serde_json::to_vec(&payload).expect("serialize consul registration"))
        .header("content-type", "application/json")
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

async fn pass_check(
    client: &reqwest::Client,
    base: &str,
    this: &NodeInfo,
) -> Result<(), reqwest::Error> {
    client
        .put(format!("{base}/v1/agent/check/pass/{}", check_id(&this.id)))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

/// Pull the healthy instances of the service and rebuild `NodeInfo` from each
/// entry's Meta, excluding this node. A malformed or role-less entry is
/// skipped, not defaulted — a garbled registration must not masquerade as a
/// node of some assumed role.
async fn fetch_members(
    client: &reqwest::Client,
    base: &str,
    self_id: &str,
) -> Result<Vec<NodeInfo>, reqwest::Error> {
    let bytes = client
        .get(format!("{base}/v1/health/service/{SERVICE}?passing=true"))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let mut out = parse_members(&bytes, self_id);
    out.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(out)
}

/// Parse Consul's `/v1/health/service` array into peers. Separate from the HTTP
/// call so it can be unit-tested on a fixture without a server.
fn parse_members(body: &[u8], self_id: &str) -> Vec<NodeInfo> {
    let value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    for entry in value.as_array().into_iter().flatten() {
        let svc = &entry["Service"];
        let id = svc["ID"].as_str().unwrap_or_default();
        if id.is_empty() || id == self_id {
            continue;
        }
        let meta = &svc["Meta"];
        let role = match meta["role"].as_str().and_then(|r| Role::parse(r).ok()) {
            Some(r) => r,
            None => continue, // role-less/garbled: skip, do not assume
        };
        out.push(NodeInfo {
            id: id.to_string(),
            role,
            address: meta["cluster_addr"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            data_address: meta["data_addr"].as_str().unwrap_or_default().to_string(),
        });
    }
    out
}

fn check_id(node_id: &str) -> String {
    format!("service:{node_id}")
}

/// Split `host:port` into its parts; a value with no colon is all host, port 0.
/// Consul's native Address/Port are informational here — identity travels in
/// Meta — so a best-effort split is fine.
fn split_host_port(addr: &str) -> (&str, u16) {
    match addr.rsplit_once(':') {
        Some((h, p)) => (h, p.parse().unwrap_or(0)),
        None => (addr, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    /// A throwaway HTTP/1.1 server that answers a GET with `members_json` and
    /// any other request (the register PUT, the check PUT) with 200. One
    /// request per connection (`Connection: close`), which is what reqwest
    /// does here. Returns its base URL.
    fn fake_consul(members_json: &'static str) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = match stream {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                // Read the request head (up to the blank line). We do not need
                // the body of the PUTs, so stopping at the header terminator is
                // enough to know the method.
                let mut buf = Vec::new();
                let mut tmp = [0u8; 512];
                loop {
                    match stream.read(&mut tmp) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            buf.extend_from_slice(&tmp[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let is_get = buf.starts_with(b"GET ");
                let body = if is_get { members_json } else { "" };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        format!("http://{addr}")
    }

    fn node(id: &str, role: Role, addr: &str, data: &str) -> NodeInfo {
        NodeInfo {
            id: id.to_string(),
            role,
            address: addr.to_string(),
            data_address: data.to_string(),
        }
    }

    const CATALOG: &str = r#"[
      {"Service":{"ID":"ing-a","Meta":{"role":"ingester","cluster_addr":"ing-a:1965","data_addr":"ing-a:1963"}}},
      {"Service":{"ID":"ing-b","Meta":{"role":"ingester","cluster_addr":"ing-b:1965","data_addr":"ing-b:1963"}}},
      {"Service":{"ID":"self","Meta":{"role":"querier","cluster_addr":"self:1965","data_addr":"self:1963"}}},
      {"Service":{"ID":"garbled","Meta":{"cluster_addr":"x:1965"}}}
    ]"#;

    #[test]
    fn parse_skips_self_and_garbled_entries() {
        let peers = parse_members(CATALOG.as_bytes(), "self");
        let ids: Vec<&str> = peers.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["ing-a", "ing-b"],
            "self excluded, role-less entry skipped"
        );
        assert_eq!(peers[0].role, Role::Ingester);
        assert_eq!(peers[0].address, "ing-a:1965");
        assert_eq!(peers[0].data_address, "ing-a:1963");
    }

    #[tokio::test]
    async fn registers_and_reports_the_healthy_members() {
        let base = fake_consul(CATALOG);
        let me = node("self", Role::Querier, "self:1965", "self:1963");
        let disc = ConsulDiscovery::start(me, &base, Duration::from_secs(60)).await;
        let peers = disc.peers();
        let ids: Vec<&str> = peers.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(ids, vec!["ing-a", "ing-b"], "reports peers, minus self");
        assert_eq!(disc.this_node().id, "self");
        assert_eq!(disc.stats().refreshes.load(Ordering::Relaxed), 1);
        assert!(!disc.stats().degraded.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn an_unreachable_consul_degrades_and_keeps_last_good() {
        // Nothing is listening on this port: the first refresh fails, so the
        // node comes up degraded with an empty (last-good) membership rather
        // than blocking or panicking.
        let disc = ConsulDiscovery::start(
            node("self", Role::Querier, "self:1965", "self:1963"),
            "http://127.0.0.1:1",
            Duration::from_secs(60),
        )
        .await;
        assert!(disc.peers().is_empty());
        assert!(
            disc.stats().degraded.load(Ordering::Relaxed),
            "degraded alarm raised"
        );
        assert_eq!(disc.stats().refreshes.load(Ordering::Relaxed), 0);
        assert!(disc.stats().refresh_failures.load(Ordering::Relaxed) >= 1);
    }
}
