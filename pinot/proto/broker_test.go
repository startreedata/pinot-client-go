package proto

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestBrokerMessages(t *testing.T) {
	req := &BrokerRequest{
		Metadata: map[string]string{"k": "v"},
		Sql:      "select 1",
	}
	req.ProtoMessage()
	require.Equal(t, "select 1", req.GetSql())
	require.Equal(t, "v", req.GetMetadata()["k"])
	require.NotEmpty(t, req.String())
	require.NotNil(t, req.ProtoReflect())
	var nilReq *BrokerRequest
	require.NotNil(t, nilReq.ProtoReflect())
	_, _ = req.Descriptor()

	resp := &BrokerResponse{
		Metadata: map[string]string{"k": "v"},
		Payload:  []byte("payload"),
	}
	resp.ProtoMessage()
	require.Equal(t, "v", resp.GetMetadata()["k"])
	require.Equal(t, []byte("payload"), resp.GetPayload())
	require.NotEmpty(t, resp.String())
	require.NotNil(t, resp.ProtoReflect())
	var nilResp *BrokerResponse
	require.NotNil(t, nilResp.ProtoReflect())
	_, _ = resp.Descriptor()

	req.Reset()
	resp.Reset()
	require.Equal(t, "", req.GetSql())
	require.Empty(t, resp.GetPayload())

	require.Empty(t, nilReq.GetSql())
	require.Nil(t, nilReq.GetMetadata())
	require.Nil(t, nilResp.GetMetadata())
	require.Nil(t, nilResp.GetPayload())

	require.NotNil(t, file_pinot_proto_broker_proto_rawDescGZIP())
	require.NotNil(t, File_pinot_proto_broker_proto)
	file_pinot_proto_broker_proto_init()
}

type testPinotServer struct {
	UnimplementedPinotQueryBrokerServer
}

func (s *testPinotServer) Submit(_ *BrokerRequest, stream PinotQueryBroker_SubmitServer) error {
	return stream.Send(&BrokerResponse{Payload: []byte("ok")})
}

func TestGrpcGeneratedClientServer(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	RegisterPinotQueryBrokerServer(server, &testPinotServer{})
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			require.NoError(t, err)
		}
	}()
	defer server.Stop()

	ctx := context.Background()
	//nolint:staticcheck // grpc.DialContext is still supported for test dialers.
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	client := NewPinotQueryBrokerClient(conn)
	stream, err := client.Submit(ctx, &BrokerRequest{Sql: "select 1"})
	require.NoError(t, err)
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), resp.Payload)
}

func TestUnimplementedServer(t *testing.T) {
	server := UnimplementedPinotQueryBrokerServer{}
	server.mustEmbedUnimplementedPinotQueryBrokerServer()
	server.testEmbeddedByValue()

	err := server.Submit(nil, nil)
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

type errClientConn struct {
	err error
}

func (c errClientConn) Invoke(context.Context, string, interface{}, interface{}, ...grpc.CallOption) error {
	return c.err
}

func (c errClientConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, c.err
}

type errClientStream struct {
	grpc.ClientStream
	sendErr  error
	closeErr error
}

func (s errClientStream) SendMsg(interface{}) error {
	return s.sendErr
}

func (s errClientStream) CloseSend() error {
	return s.closeErr
}

type errServerStream struct {
	grpc.ServerStream
	err error
}

func (s errServerStream) RecvMsg(interface{}) error {
	return s.err
}

func TestGrpcClientErrorPath(t *testing.T) {
	client := NewPinotQueryBrokerClient(errClientConn{err: status.Error(codes.Internal, "boom")})
	_, err := client.Submit(context.Background(), &BrokerRequest{Sql: "select 1"})
	require.Error(t, err)
}

type streamClientConn struct {
	stream grpc.ClientStream
}

func (c streamClientConn) Invoke(context.Context, string, interface{}, interface{}, ...grpc.CallOption) error {
	return nil
}

func (c streamClientConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.stream, nil
}

func TestGrpcClientSendErrorPath(t *testing.T) {
	client := NewPinotQueryBrokerClient(streamClientConn{stream: errClientStream{sendErr: status.Error(codes.Internal, "send")}})
	_, err := client.Submit(context.Background(), &BrokerRequest{Sql: "select 1"})
	require.Error(t, err)
}

func TestGrpcClientCloseErrorPath(t *testing.T) {
	client := NewPinotQueryBrokerClient(streamClientConn{stream: errClientStream{closeErr: status.Error(codes.Internal, "close")}})
	_, err := client.Submit(context.Background(), &BrokerRequest{Sql: "select 1"})
	require.Error(t, err)
}

func TestSubmitHandlerErrorPath(t *testing.T) {
	err := _PinotQueryBroker_Submit_Handler(&testPinotServer{}, errServerStream{err: status.Error(codes.Internal, "boom")})
	require.Error(t, err)
}
