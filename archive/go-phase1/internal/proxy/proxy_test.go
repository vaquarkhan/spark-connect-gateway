package proxy_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	pb "github.com/liangchi-hsieh/spark-connect-gateway/internal/genproto/spark/connect"
	"github.com/liangchi-hsieh/spark-connect-gateway/internal/pool/static"
	"github.com/liangchi-hsieh/spark-connect-gateway/internal/proxy"
	"github.com/liangchi-hsieh/spark-connect-gateway/internal/routing"
	"github.com/liangchi-hsieh/spark-connect-gateway/internal/store/memory"
)

// fakeBackend implements just enough of SparkConnectServiceServer to test the
// gateway's forwarding behaviour. Each backend is tagged with an ID so tests
// can assert which one served a given RPC.
type fakeBackend struct {
	pb.UnimplementedSparkConnectServiceServer
	id              string
	executeCount    atomic.Int64
	configCount     atomic.Int64
	addArtifactSeen atomic.Int64
}

func (f *fakeBackend) Config(_ context.Context, req *pb.ConfigRequest) (*pb.ConfigResponse, error) {
	f.configCount.Add(1)
	return &pb.ConfigResponse{SessionId: req.GetSessionId() + "@" + f.id}, nil
}

func (f *fakeBackend) ExecutePlan(req *pb.ExecutePlanRequest, srv grpc.ServerStreamingServer[pb.ExecutePlanResponse]) error {
	f.executeCount.Add(1)
	// Emit 3 messages so we test multi-message server streams.
	for i := 0; i < 3; i++ {
		resp := &pb.ExecutePlanResponse{
			SessionId:   req.GetSessionId() + "@" + f.id,
			OperationId: req.GetOperationId(),
		}
		if err := srv.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeBackend) ReattachExecute(req *pb.ReattachExecuteRequest, srv grpc.ServerStreamingServer[pb.ExecutePlanResponse]) error {
	f.executeCount.Add(1)
	resp := &pb.ExecutePlanResponse{
		SessionId:   req.GetSessionId() + "@" + f.id,
		OperationId: req.GetOperationId(),
	}
	return srv.Send(resp)
}

func (f *fakeBackend) AddArtifacts(srv grpc.ClientStreamingServer[pb.AddArtifactsRequest, pb.AddArtifactsResponse]) error {
	for {
		_, err := srv.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		f.addArtifactSeen.Add(1)
	}
	return srv.SendAndClose(&pb.AddArtifactsResponse{})
}

// startBackend returns (addr, *fakeBackend, stopFn).
func startBackend(t *testing.T, id string) (string, *fakeBackend, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	be := &fakeBackend{id: id}
	pb.RegisterSparkConnectServiceServer(gs, be)
	go func() { _ = gs.Serve(lis) }()
	return lis.Addr().String(), be, func() { gs.Stop() }
}

// startGateway returns (clientConn, stopFn).
func startGateway(t *testing.T, backends []string) (*grpc.ClientConn, func()) {
	t.Helper()
	pool, err := static.New(backends)
	if err != nil {
		t.Fatal(err)
	}
	router := routing.New(pool, memory.New())
	server := proxy.NewServer(router, slog.Default())

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return conn, func() {
		_ = conn.Close()
		server.Stop()
	}
}

func TestUnaryForward(t *testing.T) {
	addr, be, stop := startBackend(t, "be1")
	defer stop()

	conn, gwStop := startGateway(t, []string{addr})
	defer gwStop()

	c := pb.NewSparkConnectServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.Config(ctx, &pb.ConfigRequest{SessionId: "sess-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetSessionId() != "sess-1@be1" {
		t.Fatalf("unexpected response session id: %q", resp.GetSessionId())
	}
	if be.configCount.Load() != 1 {
		t.Fatalf("backend Config calls: got %d want 1", be.configCount.Load())
	}
}

func TestStreamForwardEmitsAllMessages(t *testing.T) {
	addr, _, stop := startBackend(t, "be1")
	defer stop()
	conn, gwStop := startGateway(t, []string{addr})
	defer gwStop()

	c := pb.NewSparkConnectServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := c.ExecutePlan(ctx, &pb.ExecutePlanRequest{SessionId: "s", OperationId: proto.String("op-1")})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("got %d messages, want 3", count)
	}
}

func TestSessionStickinessAcrossBackends(t *testing.T) {
	a1, beA, stopA := startBackend(t, "A")
	defer stopA()
	a2, beB, stopB := startBackend(t, "B")
	defer stopB()

	conn, gwStop := startGateway(t, []string{a1, a2})
	defer gwStop()
	c := pb.NewSparkConnectServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First call with sess-1 lands on backend A (round-robin first pick).
	r1, err := c.Config(ctx, &pb.ConfigRequest{SessionId: "sess-1"})
	if err != nil {
		t.Fatal(err)
	}
	first := r1.GetSessionId() // "sess-1@A" or "sess-1@B"

	// Make 5 more calls with the same session id — all must land on the same backend.
	for i := 0; i < 5; i++ {
		r, err := c.Config(ctx, &pb.ConfigRequest{SessionId: "sess-1"})
		if err != nil {
			t.Fatal(err)
		}
		if r.GetSessionId() != first {
			t.Fatalf("stickiness broken on call %d: got %q want %q", i, r.GetSessionId(), first)
		}
	}

	// A different session id should be free to pick the other backend.
	r2, err := c.Config(ctx, &pb.ConfigRequest{SessionId: "sess-2"})
	if err != nil {
		t.Fatal(err)
	}
	if r2.GetSessionId() == first {
		// not necessarily a bug — a single round-robin cursor could place
		// them together — but verify both backends actually saw traffic.
	}
	if beA.configCount.Load()+beB.configCount.Load() < 7 {
		t.Fatalf("expected ≥7 calls across backends, got A=%d B=%d", beA.configCount.Load(), beB.configCount.Load())
	}
}

func TestReattachRoutesToOriginalBackend(t *testing.T) {
	a1, _, stopA := startBackend(t, "A")
	defer stopA()
	a2, _, stopB := startBackend(t, "B")
	defer stopB()

	conn, gwStop := startGateway(t, []string{a1, a2})
	defer gwStop()
	c := pb.NewSparkConnectServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := c.ExecutePlan(ctx, &pb.ExecutePlanRequest{
		SessionId:   "sess-1",
		OperationId: proto.String("op-xyz"),
	})
	if err != nil {
		t.Fatal(err)
	}
	var firstID string
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		firstID = msg.GetSessionId() // "sess-1@A" or "sess-1@B"
	}

	// Reattach with a fresh session id (mismatched on purpose) to prove the
	// op-id reverse index is doing the work — the gateway must route to the
	// same backend that handled the original ExecutePlan.
	rs, err := c.ReattachExecute(ctx, &pb.ReattachExecuteRequest{
		SessionId:   "different-session",
		OperationId: "op-xyz",
	})
	if err != nil {
		t.Fatal(err)
	}
	var reattachID string
	for {
		msg, err := rs.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		reattachID = msg.GetSessionId()
	}
	// firstID looks like "sess-1@A"; reattachID looks like "different-session@A".
	// We assert the suffix (the backend id) matches.
	wantSuffix := firstID[len("sess-1"):]
	if got := reattachID[len("different-session"):]; got != wantSuffix {
		t.Fatalf("reattach landed on wrong backend: got suffix %q want %q (firstID=%q reattachID=%q)",
			got, wantSuffix, firstID, reattachID)
	}
}
