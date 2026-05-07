//! Routing core: SessionKey, Pool trait, AffinityStore trait, and Router.
//!
//! Backend selection is broken into three pieces so each can evolve
//! independently:
//!
//! * `Pool` — *which* backend should serve a fresh session?
//! * `AffinityStore` — *which* backend already serves an existing session?
//! * `Router` — glue that asks the store first, then the pool, and remembers
//!   the decision.

use std::sync::Arc;

/// Identifies a Spark Connect session. The pair `(user_id, session_id)` is
/// what backend Spark Connect servers themselves use to key `SparkSession`,
/// so the gateway must keep the same routing decision stable across the
/// lifetime of that pair.
///
/// `user_id` may be empty if the client did not set it; `session_id` must
/// not be empty for the affinity store to route a request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionKey {
    pub user_id: String,
    pub session_id: String,
}

impl SessionKey {
    pub fn new(user_id: impl Into<String>, session_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            session_id: session_id.into(),
        }
    }

    pub fn is_zero(&self) -> bool {
        self.session_id.is_empty()
    }
}

/// Selects backends. Implementations must be safe for concurrent use.
pub trait Pool: Send + Sync + 'static {
    fn pick(&self) -> String;
}

/// Persistence layer for sticky routing decisions. Phase 1 ships an
/// in-memory implementation; Phase 2 swaps in Redis / Postgres for HA.
pub trait AffinityStore: Send + Sync + 'static {
    fn lookup_session(&self, key: &SessionKey) -> Option<String>;
    /// Insert `(key, backend)` only if no binding for `key` exists.
    /// Returns the *winning* binding (existing or freshly inserted).
    fn bind_session_if_absent(&self, key: SessionKey, backend: String) -> String;
    fn forget_session(&self, key: &SessionKey);

    fn lookup_op(&self, op_id: &str) -> Option<String>;
    fn bind_op(&self, op_id: String, backend: String);
    fn forget_op(&self, op_id: &str);
}

/// Resolves a request to a concrete backend address.
pub struct Router {
    pool: Arc<dyn Pool>,
    store: Arc<dyn AffinityStore>,
}

impl Router {
    pub fn new(pool: Arc<dyn Pool>, store: Arc<dyn AffinityStore>) -> Self {
        Self { pool, store }
    }

    /// Resolve a backend for `key`. If a binding exists it is returned;
    /// otherwise a fresh backend is picked, recorded, and returned.
    ///
    /// A `SessionKey` with an empty `session_id` falls through to a fresh
    /// pick, but that binding is *not* recorded — without a stable session
    /// id we cannot honour stickiness on the next call.
    pub fn resolve_session(&self, key: &SessionKey) -> String {
        if key.is_zero() {
            return self.pool.pick();
        }
        if let Some(existing) = self.store.lookup_session(key) {
            return existing;
        }
        let chosen = self.pool.pick();
        self.store.bind_session_if_absent(key.clone(), chosen)
    }

    /// Resolve a backend for an operation, falling back to session routing
    /// when the operation is unknown.
    ///
    /// Used by `ReattachExecute` / `ReleaseExecute` / `Interrupt`: a client
    /// may reattach to a long-running operation that was started on a
    /// specific backend, and the gateway must route back to that same
    /// backend even if the affinity cache for the session has already
    /// expired or is missing.
    pub fn resolve_op(&self, op_id: &str, key: &SessionKey) -> String {
        if !op_id.is_empty() {
            if let Some(b) = self.store.lookup_op(op_id) {
                return b;
            }
        }
        self.resolve_session(key)
    }

    pub fn remember_op(&self, op_id: String, backend: String) {
        if op_id.is_empty() {
            return;
        }
        self.store.bind_op(op_id, backend);
    }

    pub fn forget_op(&self, op_id: &str) {
        if op_id.is_empty() {
            return;
        }
        self.store.forget_op(op_id);
    }

    pub fn forget_session(&self, key: &SessionKey) {
        self.store.forget_session(key);
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
        fn pick(&self) -> String {
            let i = self.n.fetch_add(1, Ordering::SeqCst);
            ["a", "b", "c"][(i % 3) as usize].to_string()
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
    impl AffinityStore for StubStore {
        fn lookup_session(&self, k: &SessionKey) -> Option<String> {
            self.sessions.lock().get(k).cloned()
        }
        fn bind_session_if_absent(&self, k: SessionKey, v: String) -> String {
            let mut g = self.sessions.lock();
            g.entry(k).or_insert(v).clone()
        }
        fn forget_session(&self, k: &SessionKey) {
            self.sessions.lock().remove(k);
        }
        fn lookup_op(&self, o: &str) -> Option<String> {
            self.ops.lock().get(o).cloned()
        }
        fn bind_op(&self, o: String, v: String) {
            self.ops.lock().insert(o, v);
        }
        fn forget_op(&self, o: &str) {
            self.ops.lock().remove(o);
        }
    }

    fn router() -> Router {
        Router::new(
            Arc::new(SeqPool {
                n: AtomicU64::new(0),
            }),
            Arc::new(StubStore::default()),
        )
    }

    #[test]
    fn stickiness_is_honoured() {
        let r = router();
        let k = SessionKey::new("u1", "s1");
        let first = r.resolve_session(&k);
        for _ in 0..10 {
            assert_eq!(r.resolve_session(&k), first);
        }
    }

    #[test]
    fn distinct_sessions_can_diverge() {
        let r = router();
        let a = r.resolve_session(&SessionKey::new("u1", "s1"));
        let b = r.resolve_session(&SessionKey::new("u1", "s2"));
        assert_ne!(a, b, "round-robin should diverge across sessions");
    }

    #[test]
    fn empty_session_does_not_bind() {
        let store = Arc::new(StubStore::default());
        let r = Router::new(
            Arc::new(SeqPool {
                n: AtomicU64::new(0),
            }),
            store.clone() as Arc<dyn AffinityStore>,
        );
        r.resolve_session(&SessionKey::new("u", ""));
        assert!(store
            .lookup_session(&SessionKey::new("u", "anything"))
            .is_none());
    }

    #[test]
    fn op_lookup_overrides_session() {
        let r = router();
        let k = SessionKey::new("u", "s");
        let _first = r.resolve_op("op-unknown", &k);
        r.remember_op("op-1".to_string(), "explicit:1".to_string());
        assert_eq!(r.resolve_op("op-1", &k), "explicit:1");
    }
}
