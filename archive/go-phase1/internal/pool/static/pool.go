// Package static implements a backend pool whose membership is fixed at startup.
// Phase 1 only — Phase 2 introduces dynamic K8s service-watch pools.
package static

import (
	"errors"
	"sync/atomic"
)

// Pool is a fixed-size, round-robin backend pool. Safe for concurrent use.
type Pool struct {
	backends []string
	cursor   atomic.Uint64
}

// New returns a pool over the given addresses. Order is preserved.
func New(addresses []string) (*Pool, error) {
	if len(addresses) == 0 {
		return nil, errors.New("static pool: at least one backend address required")
	}
	cp := make([]string, len(addresses))
	copy(cp, addresses)
	return &Pool{backends: cp}, nil
}

// Pick returns the next backend in round-robin order. Always succeeds when the
// pool is non-empty (which the constructor guarantees).
func (p *Pool) Pick() string {
	idx := p.cursor.Add(1) - 1
	return p.backends[int(idx%uint64(len(p.backends)))]
}

// All returns a copy of the backend list.
func (p *Pool) All() []string {
	out := make([]string, len(p.backends))
	copy(out, p.backends)
	return out
}
