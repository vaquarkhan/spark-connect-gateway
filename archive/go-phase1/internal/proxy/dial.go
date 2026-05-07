// Package proxy holds the gRPC server that fronts a pool of Spark Connect
// servers. It owns the connection cache and the per-RPC forwarding logic.
package proxy

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Dialer caches a *grpc.ClientConn per backend address so we don't pay the
// dial cost on every RPC. Phase 1 uses plaintext; Phase 2 wires TLS in.
type Dialer struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewDialer returns an empty cache.
func NewDialer() *Dialer {
	return &Dialer{conns: make(map[string]*grpc.ClientConn)}
}

// Dial returns a (possibly cached) ClientConn for addr.
func (d *Dialer) Dial(addr string) (*grpc.ClientConn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	d.conns[addr] = c
	return c, nil
}

// Close releases all cached connections.
func (d *Dialer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var firstErr error
	for _, c := range d.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	d.conns = make(map[string]*grpc.ClientConn)
	return firstErr
}
