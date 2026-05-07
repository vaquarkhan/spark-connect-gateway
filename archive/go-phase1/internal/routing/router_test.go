package routing

import (
	"sync"
	"sync/atomic"
	"testing"
)

type seqPool struct{ n atomic.Uint64 }

func (p *seqPool) Pick() string {
	idx := p.n.Add(1)
	return [...]string{"a", "b", "c"}[(idx-1)%3]
}

// stubStore is a tiny in-memory AffinityStore used only for tests, kept
// inside the routing package to avoid an import cycle with internal/store.
type stubStore struct {
	mu       sync.Mutex
	sessions map[SessionKey]string
	ops      map[string]string
}

func newStub() *stubStore {
	return &stubStore{
		sessions: make(map[SessionKey]string),
		ops:      make(map[string]string),
	}
}

func (s *stubStore) LookupSession(k SessionKey) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[k]
}

func (s *stubStore) BindSession(k SessionKey, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[k]; !exists {
		s.sessions[k] = v
	}
}

func (s *stubStore) ForgetSession(k SessionKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, k)
}

func (s *stubStore) LookupOp(o string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ops[o]
}

func (s *stubStore) BindOp(o, v string) {
	if o == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops[o] = v
}

func (s *stubStore) ForgetOp(o string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ops, o)
}

func TestRouterStickiness(t *testing.T) {
	r := New(&seqPool{}, newStub())
	k := SessionKey{UserID: "u1", SessionID: "s1"}

	first := r.ResolveSession(k)
	for i := 0; i < 10; i++ {
		if got := r.ResolveSession(k); got != first {
			t.Fatalf("call %d: lost stickiness, got %q want %q", i, got, first)
		}
	}
}

func TestRouterDifferentSessionsCanDiverge(t *testing.T) {
	r := New(&seqPool{}, newStub())
	a := r.ResolveSession(SessionKey{UserID: "u1", SessionID: "s1"})
	b := r.ResolveSession(SessionKey{UserID: "u1", SessionID: "s2"})
	if a == b {
		t.Fatalf("two distinct sessions landed on same backend %q (round-robin should diverge)", a)
	}
}

func TestRouterEmptySessionDoesNotBind(t *testing.T) {
	store := newStub()
	r := New(&seqPool{}, store)
	r.ResolveSession(SessionKey{}) // no session id
	if got := store.LookupSession(SessionKey{SessionID: "anything"}); got != "" {
		t.Fatalf("unexpected binding: %q", got)
	}
}

func TestResolveOpFallsBackToSession(t *testing.T) {
	r := New(&seqPool{}, newStub())
	k := SessionKey{UserID: "u", SessionID: "s"}

	first := r.ResolveOp("op-unknown", k)
	if first == "" {
		t.Fatal("expected a backend")
	}
	r.RememberOp("op-1", "explicit-backend:1")
	if got := r.ResolveOp("op-1", k); got != "explicit-backend:1" {
		t.Fatalf("op binding ignored, got %q", got)
	}
}
