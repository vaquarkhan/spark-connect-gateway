// Package memory provides an in-process affinity store. Phase 1 only — Phase 2
// replaces this with Redis or Postgres for cross-replica HA.
package memory

import (
	"sync"

	"github.com/liangchi-hsieh/spark-connect-gateway/internal/routing"
)

// AffinityStore maps a SessionKey (and an OperationID) to a backend address.
// The zero value is ready for use.
type AffinityStore struct {
	mu       sync.RWMutex
	sessions map[routing.SessionKey]string // session → backend
	ops      map[string]string             // operation_id → backend (reverse index for ReattachExecute)
}

// New returns an empty store.
func New() *AffinityStore {
	return &AffinityStore{
		sessions: make(map[routing.SessionKey]string),
		ops:      make(map[string]string),
	}
}

// LookupSession returns the backend bound to k, or "" if none.
func (s *AffinityStore) LookupSession(k routing.SessionKey) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[k]
}

// BindSession associates k with backend. Existing bindings are preserved
// (callers should LookupSession first to honour stickiness); BindSession is
// for inserting a fresh mapping or refreshing an identical one.
func (s *AffinityStore) BindSession(k routing.SessionKey, backend string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[k]; !exists {
		s.sessions[k] = backend
	}
}

// ForgetSession removes k. No-op if absent.
func (s *AffinityStore) ForgetSession(k routing.SessionKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, k)
}

// LookupOp returns the backend bound to operationID, or "" if none.
func (s *AffinityStore) LookupOp(operationID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ops[operationID]
}

// BindOp records operationID → backend so that ReattachExecute /
// ReleaseExecute can find the same backend even if the request lacks a
// usable session_id.
func (s *AffinityStore) BindOp(operationID, backend string) {
	if operationID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops[operationID] = backend
}

// ForgetOp removes operationID. No-op if absent.
func (s *AffinityStore) ForgetOp(operationID string) {
	if operationID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ops, operationID)
}
