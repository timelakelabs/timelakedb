//! One place where "may this request proceed?" is decided.
//!
//! HTTP and Flight SQL are different stacks with different credential
//! plumbing, but they must not be different *policies* — a token that
//! cannot read over Flight must not be able to read over `/api/sql`.
//! Both call [`decide`], so there is exactly one implementation of the
//! rule and one place to read it.

use crate::token::{DataAuthMode, TokenError, TokenIdentity};

/// What the caller wants to do. Reads and writes are separated because
/// a shipping agent should be able to write without being able to read
/// the database back — the reason `Scope` is not a total order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
}

/// The outcome of authenticating one request.
#[derive(Debug, Clone)]
pub struct Decision {
    /// `None` for an anonymous caller that the mode still permits.
    pub identity: Option<TokenIdentity>,
    /// Grants to intersect the caller's claimed SEC-2 authorizations
    /// with. `None` means "no policy recorded" and leaves claims alone;
    /// it must never be confused with "deny all", or presenting a
    /// credential would silently break a working client.
    pub granted: Option<Vec<String>>,
}

impl Decision {
    pub fn anonymous() -> Decision {
        Decision {
            identity: None,
            granted: None,
        }
    }

    pub fn is_authenticated(&self) -> bool {
        self.identity.is_some()
    }
}

/// Decide whether a request proceeds.
///
/// `presented` is the result of looking up whatever credential arrived:
/// `None` if the request carried none, `Some(Err(..))` if it carried one
/// that did not verify.
///
/// The rules, in the order they are applied:
///
/// 1. In `Off`, credentials are not examined at all — presented or not,
///    valid or garbage, the request proceeds anonymously. This is
///    today's documented contract ("write endpoints accept any
///    `Authorization` token and ignore it"), and it is what keeps the
///    compatibility promise: a Telegraf config migrated from InfluxDB
///    still carries its old token, and "existing agents write to
///    TimeLakeDB unmodified" must hold for exactly that config.
/// 2. In `Optional` and `Required`, a credential that was presented but
///    does not verify is refused. Once an operator has opted into
///    authentication, a bad token must fail loudly on day one — not on
///    the day the mode flips to `Required` and the whole fleet fails at
///    once.
/// 3. In `Optional`, a request with no credential proceeds anonymously —
///    that is the migration window.
/// 4. In `Required`, no credential is a refusal.
/// 5. A verified credential must cover the action and the database.
pub fn decide(
    mode: DataAuthMode,
    presented: Option<Result<TokenIdentity, TokenError>>,
    action: Action,
    database: &str,
) -> Result<Decision, TokenError> {
    // Rule 1: `Off` is genuinely off — the data plane does not read
    // credentials, exactly as documented today.
    if mode == DataAuthMode::Off {
        return Ok(Decision::anonymous());
    }

    let identity = match presented {
        Some(Ok(id)) => id,
        // Rule 2: once auth is on, a bad credential is refused in every
        // remaining mode.
        Some(Err(e)) => return Err(e),
        None => {
            return match mode {
                DataAuthMode::Off => unreachable!("handled above"),
                DataAuthMode::Optional => Ok(Decision::anonymous()),
                DataAuthMode::Required => Err(TokenError::Missing),
            };
        }
    };

    let permitted = match action {
        Action::Read => identity.scope.allows_read(),
        Action::Write => identity.scope.allows_write(),
    };
    if !permitted || !allows_database(&identity, database) {
        return Err(TokenError::Forbidden);
    }

    let granted = if identity.authorizations.is_empty() {
        None
    } else {
        Some(identity.authorizations.clone())
    };
    Ok(Decision {
        identity: Some(identity),
        granted,
    })
}

fn allows_database(identity: &TokenIdentity, database: &str) -> bool {
    identity.databases.is_empty() || identity.databases.iter().any(|d| d == database)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::Scope;

    fn id(scope: Scope, dbs: &[&str], auths: &[&str]) -> TokenIdentity {
        TokenIdentity {
            id: "t".into(),
            description: "d".into(),
            scope,
            databases: dbs.iter().map(|s| s.to_string()).collect(),
            authorizations: auths.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn off_and_optional_serve_anonymous_callers_required_does_not() {
        for mode in [DataAuthMode::Off, DataAuthMode::Optional] {
            let d = decide(mode, None, Action::Read, "poc").expect("anonymous allowed");
            assert!(!d.is_authenticated());
            assert!(d.granted.is_none(), "anonymous claims are left alone");
        }
        assert_eq!(
            decide(DataAuthMode::Required, None, Action::Read, "poc").err(),
            Some(TokenError::Missing)
        );
    }

    #[test]
    fn off_ignores_credentials_entirely() {
        // The compatibility promise: a Telegraf migrated from InfluxDB
        // still carries its old token, and it must keep writing
        // unmodified. Off means the header is not examined — garbage
        // and valid tokens alike are simply not looked at.
        let d = decide(
            DataAuthMode::Off,
            Some(Err(TokenError::Invalid)),
            Action::Write,
            "poc",
        )
        .expect("off ignores a garbage credential");
        assert!(!d.is_authenticated());

        let d = decide(
            DataAuthMode::Off,
            Some(Ok(id(Scope::Read, &["other"], &["ops"]))),
            Action::Write,
            "poc",
        )
        .expect("off ignores a valid credential too — even one that would be refused");
        assert!(!d.is_authenticated());
        assert!(d.granted.is_none(), "and grants nothing from it");
    }

    #[test]
    fn once_auth_is_on_a_bad_token_is_refused_in_both_modes() {
        // Opted in, a misconfigured shipper must fail loudly on day
        // one — not on the day the operator flips to `required`.
        for mode in [DataAuthMode::Optional, DataAuthMode::Required] {
            assert_eq!(
                decide(mode, Some(Err(TokenError::Invalid)), Action::Write, "poc").err(),
                Some(TokenError::Invalid),
                "mode {mode:?} must not ignore a presented-but-invalid token"
            );
        }
    }

    #[test]
    fn scope_separates_shipping_from_reading() {
        let writer = Some(Ok(id(Scope::Write, &[], &[])));
        assert!(decide(DataAuthMode::Required, writer.clone(), Action::Write, "poc").is_ok());
        assert_eq!(
            decide(DataAuthMode::Required, writer, Action::Read, "poc").err(),
            Some(TokenError::Forbidden),
            "a shipping agent's token must not read the database back"
        );

        let reader = Some(Ok(id(Scope::Read, &[], &[])));
        assert!(decide(DataAuthMode::Required, reader.clone(), Action::Read, "poc").is_ok());
        assert_eq!(
            decide(DataAuthMode::Required, reader, Action::Write, "poc").err(),
            Some(TokenError::Forbidden)
        );
    }

    #[test]
    fn database_scoping_is_enforced_and_empty_means_all() {
        let scoped = Some(Ok(id(Scope::ReadWrite, &["poc"], &[])));
        assert!(decide(DataAuthMode::Required, scoped.clone(), Action::Read, "poc").is_ok());
        assert_eq!(
            decide(DataAuthMode::Required, scoped, Action::Read, "other").err(),
            Some(TokenError::Forbidden)
        );
        let unscoped = Some(Ok(id(Scope::ReadWrite, &[], &[])));
        assert!(decide(DataAuthMode::Required, unscoped, Action::Read, "anything").is_ok());
    }

    #[test]
    fn a_token_without_grants_does_not_deny_everything() {
        // `None` grants means "no policy recorded", so claims pass
        // through. If this returned Some(vec![]) instead, every SEC-2
        // claim would be intersected to nothing and a working client
        // would go blind the moment it started authenticating.
        let d = decide(
            DataAuthMode::Required,
            Some(Ok(id(Scope::Read, &[], &[]))),
            Action::Read,
            "poc",
        )
        .expect("allowed");
        assert!(d.granted.is_none());
    }

    #[test]
    fn grants_ride_along_so_claims_can_be_narrowed() {
        let d = decide(
            DataAuthMode::Required,
            Some(Ok(id(Scope::Read, &[], &["ops", "audit"]))),
            Action::Read,
            "poc",
        )
        .expect("allowed");
        assert_eq!(
            d.granted,
            Some(vec!["ops".to_string(), "audit".to_string()])
        );
    }
}
