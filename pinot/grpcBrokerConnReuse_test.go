package pinot

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	proto "github.com/startreedata/pinot-client-go/pinot/proto"
)

// countingListener counts accepted TCP connections, i.e. how many times the
// client dialed the broker.
type countingListener struct {
	net.Listener
	accepted atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return conn, err
}

// scriptedBrokerServer answers each Submit with the result of handle.
type scriptedBrokerServer struct {
	proto.UnimplementedPinotQueryBrokerServer
	handle func(stream proto.PinotQueryBroker_SubmitServer) error
}

func (s *scriptedBrokerServer) Submit(_ *proto.BrokerRequest, stream proto.PinotQueryBroker_SubmitServer) error {
	return s.handle(stream)
}

func startCountingGrpcServer(t *testing.T, handle func(stream proto.PinotQueryBroker_SubmitServer) error) *countingListener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	counting := &countingListener{Listener: listener}
	server := grpc.NewServer()
	proto.RegisterPinotQueryBrokerServer(server, &scriptedBrokerServer{handle: handle})
	go func() {
		if serveErr := server.Serve(counting); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			assert.NoError(t, serveErr)
		}
	}()
	t.Cleanup(server.Stop)
	return counting
}

func sendOneRowResponse(t *testing.T) func(stream proto.PinotQueryBroker_SubmitServer) error {
	responses := []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: encodeTestSchema(t, []string{"id"}, []string{"LONG"})},
		{
			Payload:  encodeJSONRowBlock(t, [][]interface{}{{1}}),
			Metadata: map[string]string{"rowSize": "1", "encoding": "JSON", "compression": "NONE"},
		},
	}
	return func(stream proto.PinotQueryBroker_SubmitServer) error {
		for _, resp := range responses {
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
		return nil
	}
}

func newTestGrpcTransport(t *testing.T) *grpcBrokerClientTransport {
	t.Helper()
	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "JSON",
		Compression:  "NONE",
		BlockRowSize: 1,
		Timeout:      5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, transport.Close()) })
	return transport
}

var selectOne = &Request{queryFormat: "sql", query: "select id from t limit 1"}

func TestGrpcTransportReusesConnectionAcrossQueries(t *testing.T) {
	listener := startCountingGrpcServer(t, sendOneRowResponse(t))
	transport := newTestGrpcTransport(t)
	address := listener.Addr().String()

	for i := 0; i < 20; i++ {
		resp, err := transport.execute(address, selectOne)
		require.NoError(t, err)
		require.Equal(t, 1, resp.ResultTable.GetRowCount())
	}

	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := transport.execute(address, selectOne)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	// The "grpc://" scheme prefix must map to the same cached connection.
	_, err := transport.execute("grpc://"+address, selectOne)
	require.NoError(t, err)

	assert.Equal(t, int32(1), listener.accepted.Load(), "70 queries should share one connection")
}

func TestGrpcTransportKeepsSeparateConnectionPerBroker(t *testing.T) {
	first := startCountingGrpcServer(t, sendOneRowResponse(t))
	second := startCountingGrpcServer(t, sendOneRowResponse(t))
	transport := newTestGrpcTransport(t)

	for i := 0; i < 5; i++ {
		_, err := transport.execute(first.Addr().String(), selectOne)
		require.NoError(t, err)
		_, err = transport.execute(second.Addr().String(), selectOne)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), first.accepted.Load())
	assert.Equal(t, int32(1), second.accepted.Load())
	transport.mu.Lock()
	assert.Len(t, transport.conns, 2)
	transport.mu.Unlock()
}

func TestGrpcTransportRedialsAfterUnavailable(t *testing.T) {
	var unavailable atomic.Bool
	ok := sendOneRowResponse(t)
	listener := startCountingGrpcServer(t, func(stream proto.PinotQueryBroker_SubmitServer) error {
		if unavailable.Load() {
			return status.Error(codes.Unavailable, "broker shutting down")
		}
		return ok(stream)
	})
	transport := newTestGrpcTransport(t)
	address := listener.Addr().String()

	_, err := transport.execute(address, selectOne)
	require.NoError(t, err)

	unavailable.Store(true)
	_, err = transport.execute(address, selectOne)
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err), "wrapped error must keep the gRPC status")

	unavailable.Store(false)
	_, err = transport.execute(address, selectOne)
	require.NoError(t, err)
	assert.Equal(t, int32(2), listener.accepted.Load(), "an Unavailable failure should evict the connection")
}

func TestGrpcTransportKeepsConnectionOnQueryErrors(t *testing.T) {
	var fail atomic.Bool
	ok := sendOneRowResponse(t)
	listener := startCountingGrpcServer(t, func(stream proto.PinotQueryBroker_SubmitServer) error {
		if fail.Load() {
			return status.Error(codes.InvalidArgument, "bad sql")
		}
		return ok(stream)
	})
	transport := newTestGrpcTransport(t)
	address := listener.Addr().String()

	fail.Store(true)
	for i := 0; i < 3; i++ {
		_, err := transport.execute(address, selectOne)
		require.Error(t, err)
	}
	fail.Store(false)
	_, err := transport.execute(address, selectOne)
	require.NoError(t, err)
	assert.Equal(t, int32(1), listener.accepted.Load(), "query errors should not drop a healthy connection")
}

func TestGrpcTransportClosesEvictedConnectionAfterInFlightQueries(t *testing.T) {
	transport := newTestGrpcTransport(t)
	listener := startCountingGrpcServer(t, sendOneRowResponse(t))
	address := listener.Addr().String()

	first, err := transport.acquireConn(t.Context(), address)
	require.NoError(t, err)
	second, err := transport.acquireConn(t.Context(), address)
	require.NoError(t, err)
	require.Same(t, first, second)

	transport.releaseConn(address, first, status.Error(codes.Unavailable, "down"))
	assert.NotEqual(t, connectivity.Shutdown, first.conn.GetState(), "still in use by another query")
	transport.mu.Lock()
	assert.Empty(t, transport.conns, "evicted connection must not be handed out again")
	transport.mu.Unlock()

	transport.releaseConn(address, second, nil)
	assert.Equal(t, connectivity.Shutdown, first.conn.GetState())

	third, err := transport.acquireConn(t.Context(), address)
	require.NoError(t, err)
	assert.NotSame(t, first, third)
	transport.releaseConn(address, third, nil)
}

func TestGrpcTransportCancelsAbandonedStream(t *testing.T) {
	serverSawCancel := make(chan struct{})
	listener := startCountingGrpcServer(t, func(stream proto.PinotQueryBroker_SubmitServer) error {
		// A metadata block the client cannot decode makes it return early.
		if err := stream.Send(&proto.BrokerResponse{Payload: []byte(`{invalid`)}); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			close(serverSawCancel)
		case <-time.After(5 * time.Second):
		}
		return nil
	})
	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{Encoding: "JSON", Compression: "NONE"}) // no timeout
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, transport.Close()) })

	_, err = transport.execute(listener.Addr().String(), selectOne)
	require.Error(t, err)
	select {
	case <-serverSawCancel:
	case <-time.After(3 * time.Second):
		t.Fatal("stream was left open on the shared connection after the client gave up")
	}
}

func TestGrpcTransportClose(t *testing.T) {
	listener := startCountingGrpcServer(t, sendOneRowResponse(t))
	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{Encoding: "JSON", Compression: "NONE"})
	require.NoError(t, err)

	_, err = transport.execute(listener.Addr().String(), selectOne)
	require.NoError(t, err)
	transport.mu.Lock()
	cached := transport.conns[listener.Addr().String()]
	transport.mu.Unlock()
	require.NotNil(t, cached)

	require.NoError(t, transport.Close())
	assert.Equal(t, connectivity.Shutdown, cached.conn.GetState())
	require.NoError(t, transport.Close(), "Close must be idempotent")

	_, err = transport.execute(listener.Addr().String(), selectOne)
	assert.ErrorIs(t, err, errGrpcTransportClosed)
}

func TestGrpcTransportDialOptionErrorIsNotCached(t *testing.T) {
	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{
		TLSConfig: &GrpcTLSConfig{Enabled: true, CACertPath: t.TempDir() + "/missing.pem"},
	})
	require.NoError(t, err)
	_, err = transport.execute("127.0.0.1:1", selectOne)
	require.Error(t, err)
	_, err = transport.execute("127.0.0.1:1", selectOne)
	require.Error(t, err)
	transport.mu.Lock()
	defer transport.mu.Unlock()
	assert.Nil(t, transport.dialOptions)
	assert.Empty(t, transport.conns)
}

func TestConnectionClose(t *testing.T) {
	listener := startCountingGrpcServer(t, sendOneRowResponse(t))
	conn, err := NewWithConfig(&ClientConfig{
		BrokerList: []string{listener.Addr().String()},
		GrpcConfig: &GrpcConfig{Encoding: "JSON", Compression: "NONE", Timeout: 5 * time.Second},
	})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err = conn.ExecuteSQL("t", "select id from t limit 1")
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), listener.accepted.Load())
	require.NoError(t, conn.Close())
	_, err = conn.ExecuteSQL("t", "select id from t limit 1")
	assert.ErrorIs(t, err, errGrpcTransportClosed)

	httpConn, err := NewFromBrokerListAndClient([]string{"localhost:8000"}, http.DefaultClient)
	require.NoError(t, err)
	assert.NoError(t, httpConn.Close(), "Close is a no-op for the HTTP transport")
}
