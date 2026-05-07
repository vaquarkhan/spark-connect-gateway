package memory

import (
	"testing"

	"github.com/liangchi-hsieh/spark-connect-gateway/internal/routing"
)

func TestSessionStickiness(t *testing.T) {
	s := New()
	k := routing.SessionKey{UserID: "alice", SessionID: "sess-1"}

	if got := s.LookupSession(k); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
	s.BindSession(k, "backend-a:15002")
	if got := s.LookupSession(k); got != "backend-a:15002" {
		t.Fatalf("expected backend-a, got %q", got)
	}

	// Re-binding must not move an existing session — stickiness invariant.
	s.BindSession(k, "backend-b:15002")
	if got := s.LookupSession(k); got != "backend-a:15002" {
		t.Fatalf("expected sticky backend-a, got %q", got)
	}

	s.ForgetSession(k)
	if got := s.LookupSession(k); got != "" {
		t.Fatalf("expected empty after forget, got %q", got)
	}
}

func TestOpReverseIndex(t *testing.T) {
	s := New()

	if got := s.LookupOp("op-1"); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
	s.BindOp("op-1", "backend-a:15002")
	if got := s.LookupOp("op-1"); got != "backend-a:15002" {
		t.Fatalf("got %q", got)
	}

	// Empty op-id is a no-op.
	s.BindOp("", "backend-x")
	if got := s.LookupOp(""); got != "" {
		t.Fatalf("expected empty op-id to be ignored, got %q", got)
	}

	s.ForgetOp("op-1")
	if got := s.LookupOp("op-1"); got != "" {
		t.Fatalf("expected empty after forget, got %q", got)
	}
}
