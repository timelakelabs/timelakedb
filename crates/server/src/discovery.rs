//! Choosing a discovery backend (#71 phase 2).
//!
//! `TIMELAKE_DISCOVERY` selects the backend behind the `Arc<dyn Discovery>` the
//! rest of the server holds: unset or `static` keeps `StaticDiscovery`
//! (`TIMELAKE_PEERS`, the default, unchanged), `consul://host:port` selects the
//! live `ConsulDiscovery`. Selection is the only thing that changes here — the
//! consumers still read membership once at boot (that becomes live in phase 3).

use std::sync::Arc;
use std::time::Duration;

use timelake_cluster::{Discovery, Role, StaticDiscovery};

/// How often the Consul backend re-reads membership. Short enough that a join
/// or leave is picked up promptly, long enough not to hammer the agent.
const CONSUL_REFRESH: Duration = Duration::from_secs(5);

/// The parsed `TIMELAKE_DISCOVERY` value — pure, so the selection is testable
/// without touching the environment or the network.
#[derive(Debug, PartialEq, Eq)]
pub enum Backend {
    /// `StaticDiscovery` from `TIMELAKE_PEERS` (the default).
    Static,
    /// `ConsulDiscovery` against this HTTP base (derived from a `consul://` URL).
    Consul { base: String },
}

/// Parse `TIMELAKE_DISCOVERY`. `None`/empty and `static` → static; a
/// `consul://host:port` → Consul at `http://host:port`; anything else is a loud
/// error rather than a silent fallback to static.
pub fn parse_selector(sel: Option<&str>) -> Result<Backend, String> {
    match sel.map(str::trim).filter(|s| !s.is_empty()) {
        None => Ok(Backend::Static),
        Some(s) if s.eq_ignore_ascii_case("static") => Ok(Backend::Static),
        Some(s) if s.starts_with("consul://") => {
            let host_port = s.trim_start_matches("consul://").trim_end_matches('/');
            if host_port.is_empty() {
                return Err(
                    "TIMELAKE_DISCOVERY=consul:// needs a host:port, e.g. consul://consul:8500"
                        .to_string(),
                );
            }
            Ok(Backend::Consul {
                base: format!("http://{host_port}"),
            })
        }
        Some(other) => Err(format!(
            "TIMELAKE_DISCOVERY {other:?} is not 'static' or 'consul://host:port'"
        )),
    }
}

/// Build the discovery backend selected by `TIMELAKE_DISCOVERY`, as an
/// `Arc<dyn Discovery>` the caller can hold without caring which backend it is.
/// Async because the Consul backend registers and does a first refresh before
/// returning, so `peers()` is populated the moment it is wired in.
pub async fn from_env(role: Role) -> Result<Arc<dyn Discovery>, String> {
    match parse_selector(std::env::var("TIMELAKE_DISCOVERY").ok().as_deref())? {
        Backend::Static => {
            let d = StaticDiscovery::from_env(role).map_err(|e| e.to_string())?;
            Ok(Arc::new(d))
        }
        Backend::Consul { base } => {
            let this = timelake_cluster::this_node_from_env(role);
            tracing::info!(consul = %base, node = %this.id, "discovery backend: Consul");
            Ok(timelake_discovery::ConsulDiscovery::start(this, &base, CONSUL_REFRESH).await)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_and_static_select_the_static_backend() {
        assert_eq!(parse_selector(None), Ok(Backend::Static));
        assert_eq!(parse_selector(Some("")), Ok(Backend::Static));
        assert_eq!(parse_selector(Some("  ")), Ok(Backend::Static));
        assert_eq!(parse_selector(Some("static")), Ok(Backend::Static));
        assert_eq!(parse_selector(Some("STATIC")), Ok(Backend::Static));
    }

    #[test]
    fn a_consul_url_selects_consul_at_an_http_base() {
        assert_eq!(
            parse_selector(Some("consul://consul:8500")),
            Ok(Backend::Consul {
                base: "http://consul:8500".to_string()
            })
        );
        assert_eq!(
            parse_selector(Some("consul://127.0.0.1:8500/")),
            Ok(Backend::Consul {
                base: "http://127.0.0.1:8500".to_string()
            })
        );
    }

    #[test]
    fn a_bare_consul_scheme_or_an_unknown_value_is_a_loud_error() {
        assert!(parse_selector(Some("consul://")).is_err());
        assert!(parse_selector(Some("etcd://x:1")).is_err());
        assert!(parse_selector(Some("nonsense")).is_err());
    }
}
