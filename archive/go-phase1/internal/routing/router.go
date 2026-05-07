package routing

// Pool selects backends. Implementations must be safe for concurrent use.
type Pool interface {
	Pick() string
}

// AffinityStore is the persistence layer for sticky routing decisions.
// Phase 1 ships an in-memory implementation; Phase 2 swaps in Redis/Postgres.
type AffinityStore interface {
	LookupSession(SessionKey) string
	BindSession(SessionKey, string)
	ForgetSession(SessionKey)

	LookupOp(operationID string) string
	BindOp(operationID, backend string)
	ForgetOp(operationID string)
}

// Router resolves a request to a concrete backend address.
type Router struct {
	pool  Pool
	store AffinityStore
}

// New creates a Router backed by pool and store.
func New(pool Pool, store AffinityStore) *Router {
	return &Router{pool: pool, store: store}
}

// ResolveSession returns the backend bound to k. If no binding exists yet, a
// fresh backend is picked from the pool, recorded, and returned.
//
// SessionKey with empty SessionID falls through to a fresh pick — but that
// binding is *not* recorded, since without a stable session id we cannot
// honour stickiness on the next call. Callers should treat that case as a
// best-effort one-shot route.
func (r *Router) ResolveSession(k SessionKey) string {
	if k.IsZero() {
		return r.pool.Pick()
	}
	if existing := r.store.LookupSession(k); existing != "" {
		return existing
	}
	chosen := r.pool.Pick()
	r.store.BindSession(k, chosen)
	// LookupSession after BindSession to honour any racing writer that may
	// have bound the same key first.
	if final := r.store.LookupSession(k); final != "" {
		return final
	}
	return chosen
}

// ResolveOp returns the backend that owns the given operation, falling back
// to ResolveSession when the operation is unknown.
//
// Used by ReattachExecute / ReleaseExecute / Interrupt: a client may reattach
// to a long-running operation that was started on a specific backend, and
// the gateway must route back to that same backend even if the affinity
// cache for the session has already expired.
func (r *Router) ResolveOp(operationID string, k SessionKey) string {
	if operationID != "" {
		if b := r.store.LookupOp(operationID); b != "" {
			return b
		}
	}
	return r.ResolveSession(k)
}

// RememberOp records that operationID was started on backend.
func (r *Router) RememberOp(operationID, backend string) {
	r.store.BindOp(operationID, backend)
}

// ForgetOp drops an operation binding (e.g. on ReleaseExecute).
func (r *Router) ForgetOp(operationID string) {
	r.store.ForgetOp(operationID)
}

// ForgetSession drops a session binding (e.g. on ReleaseSession).
func (r *Router) ForgetSession(k SessionKey) {
	r.store.ForgetSession(k)
}
