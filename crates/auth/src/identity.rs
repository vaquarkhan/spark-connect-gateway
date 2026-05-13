//! The verified identity attached to an authenticated request.

use std::sync::Arc;

/// What the gateway *knows* about the caller after auth has succeeded.
///
/// `user_id` drives session-affinity routing-key construction and
/// gets injected into the forwarded `UserContext.user_id`. `tenant`
/// is consumed by the tenant resolver, per-tenant pool router, and
/// rate limiter. `groups` is recorded on session-lifecycle audit
/// events; the gateway has no RBAC layer yet, so groups are
/// informational at the data plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// Stable user identifier. Whatever the authenticator declares this
    /// to be is what gets injected into the Spark Connect
    /// `UserContext.user_id` field on forward.
    pub user_id: String,
    /// Optional tenant identifier (workspace, org, project, …).
    pub tenant: Option<String>,
    /// Optional group memberships (LDAP groups, JWT `groups` claim,
    /// custom claim).
    pub groups: Vec<String>,
}

impl Identity {
    /// Build an `Identity` with only `user_id` set.
    pub fn user(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            tenant: None,
            groups: Vec::new(),
        }
    }
}

/// Tonic request-extension wrapper. The interceptor inserts an
/// `IdentityExt(Arc<Identity>)` into the request's extensions so
/// downstream handlers can read the verified identity without
/// re-parsing headers.
#[derive(Debug, Clone)]
pub struct IdentityExt(pub Arc<Identity>);
