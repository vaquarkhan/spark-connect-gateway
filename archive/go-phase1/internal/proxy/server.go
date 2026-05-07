package proxy

import (
	"log/slog"
	"net"

	"google.golang.org/grpc"

	pb "github.com/liangchi-hsieh/spark-connect-gateway/internal/genproto/spark/connect"
	"github.com/liangchi-hsieh/spark-connect-gateway/internal/routing"
)

// Server bundles the gRPC server, its handler, and the dialer that holds
// backend connections.
type Server struct {
	GRPC    *grpc.Server
	Handler *Handler
	Dialer  *Dialer
}

// NewServer builds a configured gRPC server. It registers the SparkConnect
// service handler. The returned Server is not started; call Serve.
func NewServer(router *routing.Router, log *slog.Logger) *Server {
	dialer := NewDialer()
	h := NewHandler(router, dialer, log)
	gs := grpc.NewServer()
	pb.RegisterSparkConnectServiceServer(gs, h)
	return &Server{GRPC: gs, Handler: h, Dialer: dialer}
}

// Serve begins serving on lis. Blocks until the server stops.
func (s *Server) Serve(lis net.Listener) error {
	return s.GRPC.Serve(lis)
}

// Stop gracefully shuts down the server and closes all backend connections.
func (s *Server) Stop() {
	s.GRPC.GracefulStop()
	_ = s.Dialer.Close()
}
