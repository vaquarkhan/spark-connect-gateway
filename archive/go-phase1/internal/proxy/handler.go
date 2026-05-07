package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/liangchi-hsieh/spark-connect-gateway/internal/genproto/spark/connect"
	"github.com/liangchi-hsieh/spark-connect-gateway/internal/routing"
)

// Handler implements pb.SparkConnectServiceServer by forwarding every RPC
// to the backend chosen by the Router.
type Handler struct {
	pb.UnimplementedSparkConnectServiceServer
	router *routing.Router
	dialer *Dialer
	log    *slog.Logger
}

// NewHandler constructs a forwarding handler.
func NewHandler(router *routing.Router, dialer *Dialer, log *slog.Logger) *Handler {
	return &Handler{router: router, dialer: dialer, log: log}
}

// sessionKeyFromUserContext extracts the routing key from a request that
// carries a SessionId and an optional UserContext.
func sessionKeyFromUserContext(sessionID string, uc *pb.UserContext) routing.SessionKey {
	return routing.SessionKey{
		UserID:    uc.GetUserId(),
		SessionID: sessionID,
	}
}

// client returns a SparkConnectServiceClient bound to addr.
func (h *Handler) client(addr string) (pb.SparkConnectServiceClient, error) {
	conn, err := h.dialer.Dial(addr)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial backend %q: %v", addr, err)
	}
	return pb.NewSparkConnectServiceClient(conn), nil
}

// outboundCtx forwards the inbound metadata to the backend, dropping the few
// headers that gRPC itself owns.
func outboundCtx(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		// Strip headers gRPC will set itself on the outgoing call.
		md = md.Copy()
		md.Delete("user-agent")
		md.Delete("content-type")
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

// ----- Unary RPCs --------------------------------------------------------

func (h *Handler) AnalyzePlan(ctx context.Context, req *pb.AnalyzePlanRequest) (*pb.AnalyzePlanResponse, error) {
	addr := h.router.ResolveSession(sessionKeyFromUserContext(req.GetSessionId(), req.GetUserContext()))
	c, err := h.client(addr)
	if err != nil {
		return nil, err
	}
	return c.AnalyzePlan(outboundCtx(ctx), req)
}

func (h *Handler) Config(ctx context.Context, req *pb.ConfigRequest) (*pb.ConfigResponse, error) {
	addr := h.router.ResolveSession(sessionKeyFromUserContext(req.GetSessionId(), req.GetUserContext()))
	c, err := h.client(addr)
	if err != nil {
		return nil, err
	}
	return c.Config(outboundCtx(ctx), req)
}

func (h *Handler) ArtifactStatus(ctx context.Context, req *pb.ArtifactStatusesRequest) (*pb.ArtifactStatusesResponse, error) {
	addr := h.router.ResolveSession(sessionKeyFromUserContext(req.GetSessionId(), req.GetUserContext()))
	c, err := h.client(addr)
	if err != nil {
		return nil, err
	}
	return c.ArtifactStatus(outboundCtx(ctx), req)
}

func (h *Handler) Interrupt(ctx context.Context, req *pb.InterruptRequest) (*pb.InterruptResponse, error) {
	key := sessionKeyFromUserContext(req.GetSessionId(), req.GetUserContext())
	addr := h.router.ResolveOp(req.GetOperationId(), key)
	c, err := h.client(addr)
	if err != nil {
		return nil, err
	}
	return c.Interrupt(outboundCtx(ctx), req)
}

func (h *Handler) ReleaseExecute(ctx context.Context, req *pb.ReleaseExecuteRequest) (*pb.ReleaseExecuteResponse, error) {
	key := sessionKeyFromUserContext(req.GetSessionId(), req.GetUserContext())
	addr := h.router.ResolveOp(req.GetOperationId(), key)
	c, err := h.client(addr)
	if err != nil {
		return nil, err
	}
	resp, err := c.ReleaseExecute(outboundCtx(ctx), req)
	// On successful release, drop the op binding.
	if err == nil {
		h.router.ForgetOp(req.GetOperationId())
	}
	return resp, err
}

func (h *Handler) ReleaseSession(ctx context.Context, req *pb.ReleaseSessionRequest) (*pb.ReleaseSessionResponse, error) {
	key := sessionKeyFromUserContext(req.GetSessionId(), req.GetUserContext())
	addr := h.router.ResolveSession(key)
	c, err := h.client(addr)
	if err != nil {
		return nil, err
	}
	resp, err := c.ReleaseSession(outboundCtx(ctx), req)
	if err == nil {
		h.router.ForgetSession(key)
	}
	return resp, err
}

func (h *Handler) FetchErrorDetails(ctx context.Context, req *pb.FetchErrorDetailsRequest) (*pb.FetchErrorDetailsResponse, error) {
	addr := h.router.ResolveSession(sessionKeyFromUserContext(req.GetSessionId(), req.GetUserContext()))
	c, err := h.client(addr)
	if err != nil {
		return nil, err
	}
	return c.FetchErrorDetails(outboundCtx(ctx), req)
}

func (h *Handler) CloneSession(ctx context.Context, req *pb.CloneSessionRequest) (*pb.CloneSessionResponse, error) {
	addr := h.router.ResolveSession(sessionKeyFromUserContext(req.GetSessionId(), req.GetUserContext()))
	c, err := h.client(addr)
	if err != nil {
		return nil, err
	}
	return c.CloneSession(outboundCtx(ctx), req)
}

func (h *Handler) GetStatus(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	addr := h.router.ResolveSession(sessionKeyFromUserContext(req.GetSessionId(), req.GetUserContext()))
	c, err := h.client(addr)
	if err != nil {
		return nil, err
	}
	return c.GetStatus(outboundCtx(ctx), req)
}

// ----- Server-streaming RPCs --------------------------------------------

func (h *Handler) ExecutePlan(req *pb.ExecutePlanRequest, srv grpc.ServerStreamingServer[pb.ExecutePlanResponse]) error {
	key := sessionKeyFromUserContext(req.GetSessionId(), req.GetUserContext())
	addr := h.router.ResolveSession(key)

	// Bind the operation id to this backend so a follow-up ReattachExecute
	// from the same client routes here even if its session id is missing or
	// has been forgotten.
	if opID := req.GetOperationId(); opID != "" {
		h.router.RememberOp(opID, addr)
	}

	c, err := h.client(addr)
	if err != nil {
		return err
	}
	upstream, err := c.ExecutePlan(outboundCtx(srv.Context()), req)
	if err != nil {
		return err
	}
	return forwardServerStream(upstream, srv, h.log)
}

func (h *Handler) ReattachExecute(req *pb.ReattachExecuteRequest, srv grpc.ServerStreamingServer[pb.ExecutePlanResponse]) error {
	key := sessionKeyFromUserContext(req.GetSessionId(), req.GetUserContext())
	addr := h.router.ResolveOp(req.GetOperationId(), key)
	c, err := h.client(addr)
	if err != nil {
		return err
	}
	upstream, err := c.ReattachExecute(outboundCtx(srv.Context()), req)
	if err != nil {
		return err
	}
	return forwardServerStream(upstream, srv, h.log)
}

// ----- Client-streaming RPCs --------------------------------------------

func (h *Handler) AddArtifacts(srv grpc.ClientStreamingServer[pb.AddArtifactsRequest, pb.AddArtifactsResponse]) error {
	// We need the first message to know the session/user; routing decisions
	// are made on it, then the stream is forwarded message-by-message.
	first, err := srv.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "AddArtifacts: empty client stream")
		}
		return err
	}
	addr := h.router.ResolveSession(sessionKeyFromUserContext(first.GetSessionId(), first.GetUserContext()))
	c, err := h.client(addr)
	if err != nil {
		return err
	}
	upstream, err := c.AddArtifacts(outboundCtx(srv.Context()))
	if err != nil {
		return err
	}
	if err := upstream.Send(first); err != nil {
		return err
	}
	for {
		m, err := srv.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := upstream.Send(m); err != nil {
			return err
		}
	}
	resp, err := upstream.CloseAndRecv()
	if err != nil {
		return err
	}
	return srv.SendAndClose(resp)
}

// forwardServerStream pumps every message from upstream into srv, exiting
// when upstream is exhausted (io.EOF) or returns an error. Headers from
// upstream are forwarded once on the first message.
func forwardServerStream[T any](
	upstream grpc.ServerStreamingClient[T],
	srv interface {
		Send(*T) error
		SetHeader(metadata.MD) error
		Context() context.Context
	},
	log *slog.Logger,
) error {
	headerSent := false
	for {
		msg, err := upstream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if !headerSent {
			if md, herr := upstream.Header(); herr == nil && md.Len() > 0 {
				_ = srv.SetHeader(md)
			}
			headerSent = true
		}
		if err := srv.Send(msg); err != nil {
			if log != nil {
				log.Debug("downstream send failed", "err", err)
			}
			return err
		}
	}
}
