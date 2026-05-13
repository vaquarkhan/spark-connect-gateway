//! Routing core: SessionKey, Pool trait, AffinityStore trait,
//! TenantRouter, and Router.
//!
//! Backend selection is broken into pieces so each can evolve
//! independently:
//!
//! * `Pool` — *which* backend should serve a fresh session?
//! * `AffinityStore` — *which* backend already serves an existing session?
//! * `TenantRouter` — map a tenant string to its pool. Single-tenant
//!   deployments still work — they configure exactly one entry
//!   (often the implicit `"default"`).
//! * `Router` — glue that asks the store first, then the pool from
//!   `TenantRouter`, and remembers the decision.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::Status;

/// Identifies a Spark Connect session within a tenant. The triple
/// `(tenant, user_id, session_id)` is the affinity routing key.
/// Single-tenant deployments implicitly use the literal string
/// `"default"` for the tenant component (see [`SessionKey::new`]).
///
/// Spark Connect itself keys `SparkSession` only on `(user_id,
/// session_id)`. Adding the tenant prefix lets multiple tenants share
/// a gateway without their `session_id` namespaces colliding —
/// `(team-a, alice, sess-1)` is a different key from `(team-b,
/// alice, sess-1)`.
///
/// `user_id` may be empty if the client did not set it; `session_id`
/// must not be empty for the affinity store to route a request.
/// `tenant` is always non-empty in production (the tenant resolver
/// guarantees this).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionKey {
    pub tenant: String,
    pub user_id: String,
    pub session_id: String,
}

impl SessionKey {
    /// Build a [`SessionKey`] without an explicit tenant — used by
    /// tests and single-tenant call sites. The tenant is set to
    /// `"default"`, matching the back-compat behaviour of
    /// `TenantResolverConfig::default()`.
    pub fn new(user_id: impl Into<String>, session_id: impl Into<String>) -> Self {
        Self::with_tenant("default", user_id, session_id)
    }

    /// Build a [`SessionKey`] for an explicit tenant. This is what
    /// production handlers call after the tenant resolver yields a
    /// tenant string.
    pub fn with_tenant(
        tenant: impl Into<String>,
        user_id: impl Into<String>,
        session_id: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            user_id: user_id.into(),
            session_id: session_id.into(),
        }
    }

    pub fn is_zero(&self) -> bool {
        self.session_id.is_empty()
    }
}

/// Selects backends. Implementations must be safe for concurrent use.
///
/// Two shipping implementations: `scg-pool-static` (fixed list at
/// startup) and `scg-pool-k8s` (Endpoints watch). The K8s pool can
/// be empty during startup or after a flap, so `pick` returns
/// `Option<String>`.
pub trait Pool: Send + Sync + 'static {
    /// Pick the next backend for a *new* session, or `None` if no healthy
    /// backend is currently available. Must be safe for concurrent use;
    /// implementations typically advance an internal cursor.
    fn pick(&self) -> Option<String>;

    /// Snapshot of currently-healthy backend addresses. Used by metrics
    /// and admin endpoints. Order is implementation-defined.
    fn all_healthy(&self) -> Vec<String>;

    /// Best-effort hint that `addr` is unreachable. Implementations may
    /// remove `addr` from the rotation, decrement a health score, or
    /// ignore the hint entirely. The K8s pool uses this for passive
    /// failure detection alongside its active service-watch.
    fn mark_unhealthy(&self, _addr: &str) {}
}

/// Persistence layer for sticky routing decisions. Two shipping
/// backends: `scg-store-memory` (in-process, single-replica) and
/// `scg-store-redis` (shared across replicas for HA).
///
/// The trait is `async_trait` because the Redis backing is a
/// network call. The in-memory impl wraps its sync work in async-fn
/// signatures with no `await` points, so callers pay only the
/// trait-object dispatch cost.
#[async_trait::async_trait]
pub trait AffinityStore: Send + Sync + 'static {
    async fn lookup_session(&self, key: &SessionKey) -> Option<String>;
    /// Insert `(key, backend)` only if no binding for `key` exists.
    /// Returns the *winning* binding (existing or freshly inserted).
    async fn bind_session_if_absent(&self, key: SessionKey, backend: String) -> String;
    async fn forget_session(&self, key: &SessionKey);

    async fn lookup_op(&self, op_id: &str) -> Option<String>;
    async fn bind_op(&self, op_id: String, backend: String);
    async fn forget_op(&self, op_id: &str);
}

/// What to do when a request arrives for a tenant that has no
/// explicit pool entry. The `default` pool, if one is configured, is
/// what `UseDefault` falls back to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownTenantPolicy {
    /// Route the unknown tenant to the default pool. If no default
    /// pool is configured either, the request fails as if the pool
    /// were empty (`Unavailable`).
    UseDefault,
    /// Refuse to serve the unknown tenant — surface `PermissionDenied`
    /// to the client. SaaS-style deployments that want hard isolation
    /// between configured tenants pick this.
    Reject,
}

/// Maps a tenant string to a [`Pool`]. Construct once at startup
/// from the operator's config. Cheap to clone (everything is `Arc`).
///
/// The lookup order is:
///
/// 1. `tenants` map (exact match on tenant string).
/// 2. `default` pool if `policy == UseDefault`.
/// 3. `Err(PermissionDenied)` if `policy == Reject`.
#[derive(Clone)]
pub struct TenantRouter {
    tenants: HashMap<String, Arc<dyn Pool>>,
    default: Option<Arc<dyn Pool>>,
    policy: UnknownTenantPolicy,
}

impl TenantRouter {
    /// Build a router from explicit per-tenant pools, an optional
    /// shared default pool, and the unknown-tenant policy.
    ///
    /// `default = None` + `policy = UseDefault` is allowed but
    /// degrades to "everything except the explicit tenants fails"
    /// — equivalent to `policy = Reject` for unknown tenants, but
    /// without the explicit `PermissionDenied` (you get
    /// `Unavailable` from the empty pool path). Set the policy
    /// explicitly if you want clean error semantics.
    pub fn new(
        tenants: HashMap<String, Arc<dyn Pool>>,
        default: Option<Arc<dyn Pool>>,
        policy: UnknownTenantPolicy,
    ) -> Self {
        Self {
            tenants,
            default,
            policy,
        }
    }

    /// Single-pool convenience: every tenant routes to the same pool.
    /// Used by single-tenant deployments and by tests that don't care
    /// about per-tenant isolation.
    pub fn single(pool: Arc<dyn Pool>) -> Self {
        Self {
            tenants: HashMap::new(),
            default: Some(pool),
            policy: UnknownTenantPolicy::UseDefault,
        }
    }

    /// Pick the pool for `tenant`. Returns:
    /// * `Ok(Some(pool))` when a pool is available (explicit or default)
    /// * `Ok(None)` when no pool exists and policy says it's OK (e.g.
    ///   `UseDefault` without a default pool — caller emits the usual
    ///   "no healthy backend" error)
    /// * `Err(Status)` when `policy == Reject` and the tenant is unknown.
    pub fn pool_for(&self, tenant: &str) -> Result<Option<Arc<dyn Pool>>, Status> {
        if let Some(p) = self.tenants.get(tenant) {
            return Ok(Some(p.clone()));
        }
        match self.policy {
            UnknownTenantPolicy::UseDefault => Ok(self.default.clone()),
            UnknownTenantPolicy::Reject => Err(Status::permission_denied(format!(
                "tenant {:?} has no configured pool",
                tenant
            ))),
        }
    }

    /// Number of explicit tenants. Used by metrics / startup logs.
    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }

    /// Whether a default pool is configured.
    pub fn has_default(&self) -> bool {
        self.default.is_some()
    }
}

/// Outcome of a session-resolution call. `addr` is the backend
/// for the session; `newly_bound` distinguishes a freshly-bound
/// session (the gateway just decided which backend it lives on)
/// from an existing one (the affinity store already had the
/// binding). Used by audit logging to fire `session.create`
/// exactly once per session lifetime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveOutcome {
    pub addr: String,
    pub newly_bound: bool,
}

/// Resolves a request to a concrete backend address.
pub struct Router {
    tenants: TenantRouter,
    store: Arc<dyn AffinityStore>,
}

impl Router {
    /// Build a Router from a [`TenantRouter`] and an affinity store.
    pub fn new(tenants: TenantRouter, store: Arc<dyn AffinityStore>) -> Self {
        Self { tenants, store }
    }

    /// Single-pool convenience constructor for single-tenant
    /// deployments. Equivalent to
    /// `Router::new(TenantRouter::single(pool), store)`.
    pub fn single_pool(pool: Arc<dyn Pool>, store: Arc<dyn AffinityStore>) -> Self {
        Self::new(TenantRouter::single(pool), store)
    }

    /// Resolve a backend for `key`. If a binding exists it is returned;
    /// otherwise a fresh backend is picked from the tenant's pool,
    /// recorded, and returned.
    ///
    /// Result variants:
    ///
    /// * `Ok(Some(addr))` — a backend was found (existing binding or
    ///   freshly picked).
    /// * `Ok(None)` — no binding exists *and* the tenant's pool
    ///   currently has no healthy backend (e.g. K8s service-watch
    ///   pool during startup). Caller surfaces `Unavailable`.
    /// * `Err(Status)` — the tenant has no configured pool and the
    ///   policy is `Reject`. Caller forwards the `PermissionDenied`
    ///   directly to the client.
    ///
    /// A `SessionKey` with an empty `session_id` falls through to a
    /// fresh pick, but that binding is *not* recorded — without a
    /// stable session id we cannot honour stickiness on the next call.
    pub async fn resolve_session(&self, key: &SessionKey) -> Result<Option<String>, Status> {
        Ok(self.resolve_session_detailed(key).await?.map(|r| r.addr))
    }

    /// Same as [`resolve_session`] but distinguishes a freshly-bound
    /// session from an existing one — useful for audit logging
    /// (`session.create` events fire only on the freshly-bound
    /// path). Most callers should use `resolve_session` and ignore
    /// the binding flavour.
    pub async fn resolve_session_detailed(
        &self,
        key: &SessionKey,
    ) -> Result<Option<ResolveOutcome>, Status> {
        let Some(pool) = self.tenants.pool_for(&key.tenant)? else {
            return Ok(None);
        };
        if key.is_zero() {
            // Empty session_id is never bound — the affinity store
            // ignores it. Counts as `newly_bound = false` for audit
            // purposes (there's nothing to record).
            return Ok(pool.pick().map(|addr| ResolveOutcome {
                addr,
                newly_bound: false,
            }));
        }
        if let Some(existing) = self.store.lookup_session(key).await {
            return Ok(Some(ResolveOutcome {
                addr: existing,
                newly_bound: false,
            }));
        }
        let Some(chosen) = pool.pick() else {
            return Ok(None);
        };
        let winner = self
            .store
            .bind_session_if_absent(key.clone(), chosen.clone())
            .await;
        Ok(Some(ResolveOutcome {
            addr: winner.clone(),
            // If our `bind` returned a different value than what we
            // tried to insert, someone else won the race — we did
            // not freshly bind this session.
            newly_bound: winner == chosen,
        }))
    }

    /// Resolve a backend for an operation, falling back to session
    /// routing when the operation is unknown.
    ///
    /// Used by `ReattachExecute` / `ReleaseExecute` / `Interrupt`: a
    /// client may reattach to a long-running operation that was
    /// started on a specific backend, and the gateway must route back
    /// to that same backend even if the affinity cache for the
    /// session has already expired or is missing.
    pub async fn resolve_op(
        &self,
        op_id: &str,
        key: &SessionKey,
    ) -> Result<Option<String>, Status> {
        if !op_id.is_empty() {
            if let Some(b) = self.store.lookup_op(op_id).await {
                return Ok(Some(b));
            }
        }
        self.resolve_session(key).await
    }

    /// Hint that `addr` is currently unreachable. Routed to the pool
    /// owning `tenant`. Best-effort — unknown tenant is a no-op.
    pub fn mark_unhealthy(&self, tenant: &str, addr: &str) {
        if let Ok(Some(pool)) = self.tenants.pool_for(tenant) {
            pool.mark_unhealthy(addr);
        }
    }

    pub async fn remember_op(&self, op_id: String, backend: String) {
        if op_id.is_empty() {
            return;
        }
        self.store.bind_op(op_id, backend).await;
    }

    pub async fn forget_op(&self, op_id: &str) {
        if op_id.is_empty() {
            return;
        }
        self.store.forget_op(op_id).await;
    }

    pub async fn forget_session(&self, key: &SessionKey) {
        self.store.forget_session(key).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex as PLMutex;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct SeqPool {
        n: AtomicU64,
    }
    impl Pool for SeqPool {
        fn pick(&self) -> Option<String> {
            let i = self.n.fetch_add(1, Ordering::SeqCst);
            Some(["a", "b", "c"][(i % 3) as usize].to_string())
        }
        fn all_healthy(&self) -> Vec<String> {
            vec!["a".into(), "b".into(), "c".into()]
        }
    }

    /// Pool that always reports empty — used to test the "no backend"
    /// path through Router::resolve_session.
    struct EmptyPool;
    impl Pool for EmptyPool {
        fn pick(&self) -> Option<String> {
            None
        }
        fn all_healthy(&self) -> Vec<String> {
            Vec::new()
        }
    }

    struct StubStore {
        sessions: PLMutex<HashMap<SessionKey, String>>,
        ops: PLMutex<HashMap<String, String>>,
    }
    impl Default for StubStore {
        fn default() -> Self {
            Self {
                sessions: PLMutex::new(HashMap::new()),
                ops: PLMutex::new(HashMap::new()),
            }
        }
    }
    #[async_trait::async_trait]
    impl AffinityStore for StubStore {
        async fn lookup_session(&self, k: &SessionKey) -> Option<String> {
            self.sessions.lock().get(k).cloned()
        }
        async fn bind_session_if_absent(&self, k: SessionKey, v: String) -> String {
            let mut g = self.sessions.lock();
            g.entry(k).or_insert(v).clone()
        }
        async fn forget_session(&self, k: &SessionKey) {
            self.sessions.lock().remove(k);
        }
        async fn lookup_op(&self, o: &str) -> Option<String> {
            self.ops.lock().get(o).cloned()
        }
        async fn bind_op(&self, o: String, v: String) {
            self.ops.lock().insert(o, v);
        }
        async fn forget_op(&self, o: &str) {
            self.ops.lock().remove(o);
        }
    }

    fn router() -> Router {
        Router::single_pool(
            Arc::new(SeqPool {
                n: AtomicU64::new(0),
            }),
            Arc::new(StubStore::default()),
        )
    }

    #[tokio::test]
    async fn stickiness_is_honoured() {
        let r = router();
        let k = SessionKey::new("u1", "s1");
        let first = r.resolve_session(&k).await.unwrap();
        for _ in 0..10 {
            assert_eq!(r.resolve_session(&k).await.unwrap(), first);
        }
    }

    #[tokio::test]
    async fn distinct_sessions_can_diverge() {
        let r = router();
        let a = r
            .resolve_session(&SessionKey::new("u1", "s1"))
            .await
            .unwrap();
        let b = r
            .resolve_session(&SessionKey::new("u1", "s2"))
            .await
            .unwrap();
        assert_ne!(a, b, "round-robin should diverge across sessions");
    }

    #[tokio::test]
    async fn empty_session_does_not_bind() {
        let store = Arc::new(StubStore::default());
        let r = Router::single_pool(
            Arc::new(SeqPool {
                n: AtomicU64::new(0),
            }),
            store.clone() as Arc<dyn AffinityStore>,
        );
        r.resolve_session(&SessionKey::new("u", "")).await.unwrap();
        assert!(store
            .lookup_session(&SessionKey::new("u", "anything"))
            .await
            .is_none());
    }

    #[tokio::test]
    async fn op_lookup_overrides_session() {
        let r = router();
        let k = SessionKey::new("u", "s");
        let _first = r.resolve_op("op-unknown", &k).await.unwrap();
        r.remember_op("op-1".to_string(), "explicit:1".to_string())
            .await;
        assert_eq!(
            r.resolve_op("op-1", &k).await.unwrap().as_deref(),
            Some("explicit:1")
        );
    }

    #[tokio::test]
    async fn empty_pool_returns_none() {
        let r = Router::single_pool(Arc::new(EmptyPool), Arc::new(StubStore::default()));
        let k = SessionKey::new("u", "s");
        assert!(r.resolve_session(&k).await.unwrap().is_none());
        assert!(r.resolve_op("op", &k).await.unwrap().is_none());
    }

    // ---- TenantRouter tests -----------------------------------------

    fn fixed_pool(addr: &'static str) -> Arc<dyn Pool> {
        struct FixedPool(&'static str);
        impl Pool for FixedPool {
            fn pick(&self) -> Option<String> {
                Some(self.0.to_string())
            }
            fn all_healthy(&self) -> Vec<String> {
                vec![self.0.to_string()]
            }
        }
        Arc::new(FixedPool(addr))
    }

    #[tokio::test]
    async fn tenant_router_picks_per_tenant_pool() {
        let mut tenants = HashMap::new();
        tenants.insert("team-a".to_string(), fixed_pool("a:1"));
        tenants.insert("team-b".to_string(), fixed_pool("b:1"));
        let tr = TenantRouter::new(tenants, None, UnknownTenantPolicy::Reject);
        let store: Arc<dyn AffinityStore> = Arc::new(StubStore::default());
        let r = Router::new(tr, store);

        let got_a = r
            .resolve_session(&SessionKey::with_tenant("team-a", "u", "s1"))
            .await
            .unwrap();
        assert_eq!(got_a.as_deref(), Some("a:1"));

        let got_b = r
            .resolve_session(&SessionKey::with_tenant("team-b", "u", "s1"))
            .await
            .unwrap();
        assert_eq!(got_b.as_deref(), Some("b:1"));
    }

    #[tokio::test]
    async fn tenant_router_falls_back_to_default_when_use_default() {
        let mut tenants = HashMap::new();
        tenants.insert("team-a".to_string(), fixed_pool("a:1"));
        let tr = TenantRouter::new(
            tenants,
            Some(fixed_pool("default:1")),
            UnknownTenantPolicy::UseDefault,
        );
        let store: Arc<dyn AffinityStore> = Arc::new(StubStore::default());
        let r = Router::new(tr, store);

        let got = r
            .resolve_session(&SessionKey::with_tenant("unknown-tenant", "u", "s1"))
            .await
            .unwrap();
        assert_eq!(got.as_deref(), Some("default:1"));
    }

    #[tokio::test]
    async fn tenant_router_rejects_unknown_tenant_under_reject_policy() {
        let mut tenants = HashMap::new();
        tenants.insert("team-a".to_string(), fixed_pool("a:1"));
        let tr = TenantRouter::new(tenants, None, UnknownTenantPolicy::Reject);
        let store: Arc<dyn AffinityStore> = Arc::new(StubStore::default());
        let r = Router::new(tr, store);

        let err = r
            .resolve_session(&SessionKey::with_tenant("unknown", "u", "s1"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn tenant_router_no_default_no_explicit_returns_none() {
        // UseDefault policy + no default pool + unknown tenant → Ok(None).
        // The caller surfaces this as the usual "no healthy backend" error.
        let tr = TenantRouter::new(HashMap::new(), None, UnknownTenantPolicy::UseDefault);
        let store: Arc<dyn AffinityStore> = Arc::new(StubStore::default());
        let r = Router::new(tr, store);
        let got = r
            .resolve_session(&SessionKey::with_tenant("nobody", "u", "s1"))
            .await
            .unwrap();
        assert!(got.is_none());
    }
}
