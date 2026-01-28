package pinot

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	proto "github.com/startreedata/pinot-client-go/pinot/proto"
)

type mockPinotQueryBrokerServer struct {
	proto.UnimplementedPinotQueryBrokerServer
	responses   []*proto.BrokerResponse
	lastRequest *proto.BrokerRequest
}

func (s *mockPinotQueryBrokerServer) Submit(req *proto.BrokerRequest, stream proto.PinotQueryBroker_SubmitServer) error {
	s.lastRequest = req
	for _, resp := range s.responses {
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

type errorPinotQueryBrokerServer struct {
	proto.UnimplementedPinotQueryBrokerServer
	responses []*proto.BrokerResponse
	err       error
}

func (s *errorPinotQueryBrokerServer) Submit(_ *proto.BrokerRequest, stream proto.PinotQueryBroker_SubmitServer) error {
	for _, resp := range s.responses {
		if sendErr := stream.Send(resp); sendErr != nil {
			return sendErr
		}
	}
	return s.err
}

func startGrpcTestServer(t *testing.T, responses []*proto.BrokerResponse) (*grpc.Server, net.Listener, *mockPinotQueryBrokerServer) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	server := grpc.NewServer()
	mockServer := &mockPinotQueryBrokerServer{responses: responses}
	proto.RegisterPinotQueryBrokerServer(server, mockServer)
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			assert.NoError(t, serveErr)
		}
	}()
	return server, listener, mockServer
}

func startGrpcErrorServer(t *testing.T, responses []*proto.BrokerResponse, submitErr error) (*grpc.Server, net.Listener) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	server := grpc.NewServer()
	proto.RegisterPinotQueryBrokerServer(server, &errorPinotQueryBrokerServer{responses: responses, err: submitErr})
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			assert.NoError(t, serveErr)
		}
	}()
	return server, listener
}

func TestGrpcBrokerClientTransportJSON(t *testing.T) {
	schemaPayload := encodeTestSchema(t, []string{"id"}, []string{"LONG"})
	rowPayload := encodeJSONRowBlock(t, [][]interface{}{{json.Number("1")}, {json.Number("2")}})
	responses := []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: schemaPayload},
		{
			Payload: rowPayload,
			Metadata: map[string]string{
				"rowSize":     "2",
				"encoding":    "JSON",
				"compression": "NONE",
			},
		},
	}

	server, listener, mockServer := startGrpcTestServer(t, responses)
	defer server.Stop()

	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "JSON",
		Compression:  "NONE",
		BlockRowSize: 2,
		Timeout:      time.Second,
	})
	assert.NoError(t, err)

	resp, err := transport.execute(listener.Addr().String(), &Request{
		queryFormat:         "sql",
		query:               "select * from baseballStats limit 2",
		useMultistageEngine: true,
		trace:               true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.ResultTable)
	assert.Equal(t, 2, resp.ResultTable.GetRowCount())
	assert.Equal(t, json.Number("1"), resp.ResultTable.Rows[0][0])
	assert.Equal(t, json.Number("2"), resp.ResultTable.Rows[1][0])

	assert.NotNil(t, mockServer.lastRequest)
	assert.Equal(t, "JSON", mockServer.lastRequest.Metadata["encoding"])
	assert.Equal(t, "NONE", mockServer.lastRequest.Metadata["compression"])
	assert.Equal(t, "2", mockServer.lastRequest.Metadata["blockRowSize"])
	assert.Contains(t, mockServer.lastRequest.Metadata["queryOptions"], "groupByMode=sql")
	assert.Contains(t, mockServer.lastRequest.Metadata["queryOptions"], "useMultistageEngine=true")
	assert.Equal(t, "true", mockServer.lastRequest.Metadata["trace"])
}

func TestGrpcBrokerClientTransportArrow(t *testing.T) {
	schemaPayload := encodeTestSchema(t, []string{"id"}, []string{"LONG"})
	rowPayload := encodeArrowRowBlock(t, []int64{5, 6})
	responses := []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: schemaPayload},
		{
			Payload: rowPayload,
			Metadata: map[string]string{
				"rowSize":     "2",
				"encoding":    "ARROW",
				"compression": "NONE",
			},
		},
	}

	server, listener, _ := startGrpcTestServer(t, responses)
	defer server.Stop()

	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "ARROW",
		Compression:  "NONE",
		BlockRowSize: 2,
		Timeout:      time.Second,
	})
	assert.NoError(t, err)

	resp, err := transport.execute(listener.Addr().String(), &Request{
		queryFormat: "sql",
		query:       "select * from baseballStats limit 2",
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, resp.ResultTable.GetRowCount())
	assert.Equal(t, json.Number("5"), resp.ResultTable.Rows[0][0])
	assert.Equal(t, json.Number("6"), resp.ResultTable.Rows[1][0])
}

func TestGrpcBrokerClientTransportErrors(t *testing.T) {
	schemaPayload := encodeTestSchema(t, []string{"id"}, []string{"LONG"})
	rowPayload := encodeJSONRowBlock(t, [][]interface{}{{json.Number("1")}})

	server, listener, _ := startGrpcTestServer(t, []*proto.BrokerResponse{})
	defer server.Stop()

	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "JSON",
		Compression:  "NONE",
		BlockRowSize: 1,
		Timeout:      time.Second,
	})
	require.NoError(t, err)

	_, err = transport.execute(listener.Addr().String(), &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)

	server.Stop()
	server, listener, _ = startGrpcTestServer(t, []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: schemaPayload},
		{
			Payload: rowPayload,
			Metadata: map[string]string{
				"encoding":    "BAD",
				"compression": "NONE",
				"rowSize":     "1",
			},
		},
	})
	defer server.Stop()

	_, err = transport.execute(listener.Addr().String(), &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)

	server.Stop()
	server, listener, _ = startGrpcTestServer(t, []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: []byte{0x1}},
		{
			Payload: rowPayload,
			Metadata: map[string]string{
				"encoding":    "JSON",
				"compression": "UNKNOWN",
				"rowSize":     "1",
			},
		},
	})
	defer server.Stop()

	_, err = transport.execute(listener.Addr().String(), &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)

	server.Stop()
	server, listener, _ = startGrpcTestServer(t, []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: schemaPayload},
		{
			Payload: rowPayload,
			Metadata: map[string]string{
				"encoding":    "JSON",
				"compression": "UNKNOWN",
				"rowSize":     "1",
			},
		},
	})
	defer server.Stop()

	_, err = transport.execute(listener.Addr().String(), &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)

	server.Stop()
	server, listener, _ = startGrpcTestServer(t, []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: schemaPayload},
		{
			Payload: rowPayload,
			Metadata: map[string]string{
				"encoding":    "JSON",
				"compression": "NONE",
			},
		},
	})
	defer server.Stop()

	_, err = transport.execute(listener.Addr().String(), &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)

	server.Stop()
	server, listener, _ = startGrpcTestServer(t, []*proto.BrokerResponse{
		{Payload: []byte(`{invalid`)},
	})
	defer server.Stop()

	_, err = transport.execute(listener.Addr().String(), &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)

	server.Stop()
	server, listener = startGrpcErrorServer(t, nil, errors.New("submit failed"))
	defer server.Stop()
	_, err = transport.execute(listener.Addr().String(), &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)

	server.Stop()
	server, listener = startGrpcErrorServer(t, []*proto.BrokerResponse{{Payload: []byte(`{"exceptions":[]}`)}}, errors.New("stream failed"))
	defer server.Stop()
	_, err = transport.execute(listener.Addr().String(), &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)

	transport, err = newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "JSON",
		Compression:  "NONE",
		BlockRowSize: 1,
		Timeout:      time.Millisecond,
	})
	require.NoError(t, err)

	_, err = transport.execute("bad:address", &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)

	transport, err = newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "JSON",
		Compression:  "NONE",
		BlockRowSize: 1,
		Timeout:      time.Millisecond,
		TLSConfig: &GrpcTLSConfig{
			Enabled:    true,
			CACertPath: "/tmp/does-not-exist",
		},
	})
	require.NoError(t, err)
	_, err = transport.execute(listener.Addr().String(), &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)

	server.Stop()
	server, listener, _ = startGrpcTestServer(t, []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: schemaPayload},
		{
			Payload:  rowPayload,
			Metadata: nil,
		},
	})
	defer server.Stop()
	_, err = transport.execute(listener.Addr().String(), &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)
}

func TestGrpcBrokerClientTransportDialError(t *testing.T) {
	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "JSON",
		Compression:  "NONE",
		BlockRowSize: 1,
		Timeout:      time.Second,
	})
	require.NoError(t, err)

	original := grpcDialContext
	grpcDialContext = func(_ context.Context, _ string, _ ...grpc.DialOption) (*grpc.ClientConn, error) {
		return nil, errors.New("dial failed")
	}
	t.Cleanup(func() { grpcDialContext = original })

	_, err = transport.execute("localhost:1234", &Request{queryFormat: "sql", query: "select 1"})
	assert.Error(t, err)
}

func TestNewGrpcBrokerClientTransportErrors(t *testing.T) {
	transport, err := newGrpcBrokerClientTransport(nil)
	assert.Nil(t, transport)
	assert.Error(t, err)
}

func TestBuildGrpcDialOptions(t *testing.T) {
	options, err := buildGrpcDialOptions(&GrpcConfig{})
	assert.NoError(t, err)
	assert.NotEmpty(t, options)

	options, err = buildGrpcDialOptions(&GrpcConfig{
		TLSConfig: &GrpcTLSConfig{
			Enabled:            true,
			InsecureSkipVerify: true,
		},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, options)

	temp := t.TempDir()
	_, err = buildGrpcDialOptions(&GrpcConfig{
		TLSConfig: &GrpcTLSConfig{
			Enabled:    true,
			CACertPath: temp + "/missing.pem",
		},
	})
	assert.Error(t, err)

	certPath := temp + "/invalid.pem"
	require.NoError(t, os.WriteFile(certPath, []byte("not pem"), 0o600))
	_, err = buildGrpcDialOptions(&GrpcConfig{
		TLSConfig: &GrpcTLSConfig{
			Enabled:    true,
			CACertPath: certPath,
		},
	})
	assert.Error(t, err)

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privKey.PublicKey, privKey)
	require.NoError(t, err)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	validPath := temp + "/valid.pem"
	require.NoError(t, os.WriteFile(validPath, pemBytes, 0o600))
	options, err = buildGrpcDialOptions(&GrpcConfig{
		TLSConfig: &GrpcTLSConfig{
			Enabled:    true,
			CACertPath: validPath,
		},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, options)
}

func TestNormalizeGrpcAddress(t *testing.T) {
	assert.Equal(t, "localhost:9000", normalizeGrpcAddress("grpc://localhost:9000"))
	assert.Equal(t, "localhost:9000", normalizeGrpcAddress("grpcs://localhost:9000"))
	assert.Equal(t, "localhost:9000", normalizeGrpcAddress("localhost:9000"))
}

func TestBuildGrpcMetadataDefaults(t *testing.T) {
	metadata := buildGrpcMetadata(&GrpcConfig{
		BlockRowSize: 0,
		Timeout:      3 * time.Second,
		ExtraMetadata: map[string]string{
			"extra": "value",
		},
	}, &Request{
		queryFormat:         "sql",
		useMultistageEngine: true,
		trace:               true,
	})

	assert.Equal(t, "10000", metadata["blockRowSize"])
	assert.Equal(t, "JSON", metadata["encoding"])
	assert.Equal(t, "ZSTD", metadata["compression"])
	assert.Equal(t, "value", metadata["extra"])
	assert.Contains(t, metadata["queryOptions"], "groupByMode=sql")
	assert.Contains(t, metadata["queryOptions"], "useMultistageEngine=true")
	assert.Contains(t, metadata["queryOptions"], "timeoutMs=")
	assert.Equal(t, "true", metadata["trace"])
}

func TestBuildGrpcQueryOptionsNonSQL(t *testing.T) {
	options := buildGrpcQueryOptions(&Request{
		queryFormat:         "pql",
		useMultistageEngine: true,
	}, 0)
	assert.Equal(t, "useMultistageEngine=true", options)
}

func TestNormalizeAlgorithm(t *testing.T) {
	assert.Equal(t, "A", normalizeAlgorithm("A", "B", "C"))
	assert.Equal(t, "B", normalizeAlgorithm("", "B", "C"))
	assert.Equal(t, "C", normalizeAlgorithm("", "", "C"))
}

func TestParseRowSizeErrors(t *testing.T) {
	_, err := parseRowSize(nil)
	assert.Error(t, err)

	_, err = parseRowSize(map[string]string{})
	assert.Error(t, err)

	_, err = parseRowSize(map[string]string{"rowSize": "abc"})
	assert.Error(t, err)

	val, err := parseRowSize(map[string]string{"rowSize": "2"})
	assert.NoError(t, err)
	assert.Equal(t, 2, val)
}

func TestDecodeDataSchemaErrors(t *testing.T) {
	_, err := decodeDataSchema([]byte{})
	assert.Error(t, err)

	buf := &bytes.Buffer{}
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(-1)))
	_, err = decodeDataSchema(buf.Bytes())
	assert.Error(t, err)

	buf.Reset()
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(1)))
	_, err = decodeDataSchema(buf.Bytes())
	assert.Error(t, err)

	buf.Reset()
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(1)))
	writeSchemaString(t, buf, "col")
	_, err = decodeDataSchema(buf.Bytes())
	assert.Error(t, err)
}

func TestReadSchemaStringErrors(t *testing.T) {
	buf := &bytes.Buffer{}
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(-1)))
	_, err := readSchemaString(bytes.NewReader(buf.Bytes()))
	assert.Error(t, err)

	buf.Reset()
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(4)))
	_, err = readSchemaString(bytes.NewReader(buf.Bytes()))
	assert.Error(t, err)
}

func TestDecodeJSONRowsErrors(t *testing.T) {
	_, err := decodeJSONRows([]byte{}, 1)
	assert.Error(t, err)

	buf := &bytes.Buffer{}
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(-1)))
	_, err = decodeJSONRows(buf.Bytes(), 1)
	assert.Error(t, err)

	buf.Reset()
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(3)))
	_, err = decodeJSONRows(buf.Bytes(), 1)
	assert.Error(t, err)

	buf.Reset()
	rowBytes := []byte("{invalid")
	writeInt32(t, buf, len(rowBytes))
	_, err = buf.Write(rowBytes)
	require.NoError(t, err)
	_, err = decodeJSONRows(buf.Bytes(), 1)
	assert.Error(t, err)
}

func TestDecodeMap(t *testing.T) {
	_, err := decodeMap([]byte{})
	assert.Error(t, err)

	buf := &bytes.Buffer{}
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(0)))
	output, err := decodeMap(buf.Bytes())
	assert.NoError(t, err)
	assert.Empty(t, output)

	buf.Reset()
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(1)))
	writeSchemaString(t, buf, "k")
	valueBytes, err := json.Marshal(map[string]interface{}{"a": 1})
	require.NoError(t, err)
	writeInt32(t, buf, len(valueBytes))
	_, err = buf.Write(valueBytes)
	require.NoError(t, err)
	output, err = decodeMap(buf.Bytes())
	assert.NoError(t, err)
	assert.Contains(t, output, "k")

	buf.Reset()
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(-1)))
	_, err = decodeMap(buf.Bytes())
	assert.Error(t, err)

	buf.Reset()
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(1)))
	writeSchemaString(t, buf, "k")
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(-1)))
	_, err = decodeMap(buf.Bytes())
	assert.Error(t, err)

	buf.Reset()
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(1)))
	writeSchemaString(t, buf, "k")
	invalidBytes := []byte("{invalid")
	writeInt32(t, buf, len(invalidBytes))
	_, err = buf.Write(invalidBytes)
	require.NoError(t, err)
	_, err = decodeMap(buf.Bytes())
	assert.Error(t, err)

	buf.Reset()
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(1)))
	writeSchemaString(t, buf, "k")
	_, err = buf.Write([]byte{0x1, 0x2})
	require.NoError(t, err)
	_, err = decodeMap(buf.Bytes())
	assert.Error(t, err)

	buf.Reset()
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(1)))
	writeSchemaString(t, buf, "k")
	writeInt32(t, buf, 5)
	_, err = buf.Write([]byte{1, 2})
	require.NoError(t, err)
	_, err = decodeMap(buf.Bytes())
	assert.Error(t, err)

	buf.Reset()
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(1)))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(3)))
	_, err = decodeMap(buf.Bytes())
	assert.Error(t, err)
}

func TestDecompressGrpcPayloadAlgorithms(t *testing.T) {
	payload := bytes.Repeat([]byte("grpc-compress"), 50)

	compressed := snappy.Encode(nil, payload)
	out, err := decompressGrpcPayload(compressed, "SNAPPY")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	buf := &bytes.Buffer{}
	writer := zlib.NewWriter(buf)
	_, err = writer.Write(payload)
	assert.NoError(t, err)
	assert.NoError(t, writer.Close())
	out, err = decompressGrpcPayload(buf.Bytes(), "DEFLATE")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	buf.Reset()
	lz4Writer := lz4.NewWriter(buf)
	_, err = lz4Writer.Write(payload)
	assert.NoError(t, err)
	assert.NoError(t, lz4Writer.Close())
	out, err = decompressGrpcPayload(buf.Bytes(), "LZ4")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	out, err = decompressGrpcPayload(buf.Bytes(), "LZ4_FAST")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	out, err = decompressGrpcPayload(buf.Bytes(), "LZ4_HIGH")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	lz4Raw := make([]byte, lz4.CompressBlockBound(len(payload)))
	lz4Size, err := lz4.CompressBlock(payload, lz4Raw, nil)
	require.NoError(t, err)
	require.Greater(t, lz4Size, 0)
	lz4Prefixed := make([]byte, 4+lz4Size)
	binary.BigEndian.PutUint32(lz4Prefixed[:4], requireUint32Length(t, len(payload)))
	copy(lz4Prefixed[4:], lz4Raw[:lz4Size])
	out, err = decompressGrpcPayload(lz4Prefixed, "LZ4")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	out, err = decompressGrpcPayload(payload, "NONE")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	out, err = decompressGrpcPayload(payload, "PASS_THROUGH")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	out, err = decompressGrpcPayload(payload, "")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	encoder, err := zstd.NewWriter(nil)
	assert.NoError(t, err)
	compressed = encoder.EncodeAll(payload, nil)
	assert.NoError(t, encoder.Close())
	out, err = decompressGrpcPayload(compressed, "ZSTANDARD")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	out, err = decompressGrpcPayload(compressed, "ZSTD")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	zstdPrefixed := make([]byte, 4+len(compressed))
	binary.BigEndian.PutUint32(zstdPrefixed[:4], requireUint32Length(t, len(payload)))
	copy(zstdPrefixed[4:], compressed)
	out, err = decompressGrpcPayload(zstdPrefixed, "ZSTD")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	buf.Reset()
	gzipWriter := gzip.NewWriter(buf)
	_, err = gzipWriter.Write(payload)
	assert.NoError(t, err)
	assert.NoError(t, gzipWriter.Close())
	out, err = decompressGrpcPayload(buf.Bytes(), "GZIP")
	assert.NoError(t, err)
	assert.Equal(t, payload, out)

	_, err = decompressGrpcPayload(payload, "UNKNOWN")
	assert.Error(t, err)

	_, err = decompressGrpcPayload([]byte("invalid"), "SNAPPY")
	assert.Error(t, err)
}

func TestDecompressGrpcPayloadLengthPrefixedErrors(t *testing.T) {
	short := []byte{1, 2, 3}
	_, err := decompressGrpcPayload(short, "ZSTD")
	assert.Error(t, err)

	_, err = decompressGrpcPayload(short, "LZ4")
	assert.Error(t, err)

	invalidZstd := []byte("notzstd")
	zstdPrefixed := make([]byte, 4+len(invalidZstd))
	binary.BigEndian.PutUint32(zstdPrefixed[:4], requireUint32Length(t, 10))
	copy(zstdPrefixed[4:], invalidZstd)
	_, err = decompressGrpcPayload(zstdPrefixed, "ZSTD")
	assert.Error(t, err)

	payload := bytes.Repeat([]byte("lz4-mismatch"), 25)
	lz4Raw := make([]byte, lz4.CompressBlockBound(len(payload)))
	lz4Size, err := lz4.CompressBlock(payload, lz4Raw, nil)
	require.NoError(t, err)
	require.Greater(t, lz4Size, 0)
	lz4Prefixed := make([]byte, 4+lz4Size)
	binary.BigEndian.PutUint32(lz4Prefixed[:4], requireUint32Length(t, len(payload)+1))
	copy(lz4Prefixed[4:], lz4Raw[:lz4Size])
	_, err = decompressGrpcPayload(lz4Prefixed, "LZ4")
	assert.Error(t, err)

	lz4Invalid := make([]byte, 4+3)
	binary.BigEndian.PutUint32(lz4Invalid[:4], requireUint32Length(t, 5))
	copy(lz4Invalid[4:], []byte{1, 2, 3})
	_, err = decompressGrpcPayload(lz4Invalid, "LZ4")
	assert.Error(t, err)

	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := encoder.EncodeAll(payload, nil)
	require.NoError(t, encoder.Close())
	zstdMismatch := make([]byte, 4+len(compressed))
	binary.BigEndian.PutUint32(zstdMismatch[:4], requireUint32Length(t, len(payload)+1))
	copy(zstdMismatch[4:], compressed)
	_, err = decompressGrpcPayload(zstdMismatch, "ZSTD")
	assert.Error(t, err)

	lz4Zero := []byte{0, 0, 0, 0, 1}
	out, err := decompressGrpcPayload(lz4Zero, "LZ4")
	assert.NoError(t, err)
	assert.Empty(t, out)
}

func TestDecompressGrpcPayloadErrors(t *testing.T) {
	_, err := decompressGrpcPayload([]byte("notgzip"), "GZIP")
	assert.Error(t, err)
	_, err = decompressGrpcPayload([]byte("notdeflate"), "DEFLATE")
	assert.Error(t, err)
	_, err = decompressGrpcPayload([]byte("notzstd"), "ZSTD")
	assert.Error(t, err)
}

func TestDecompressGrpcPayloadWrapper(t *testing.T) {
	payload := []byte("wrap")
	out, err := DecompressGrpcPayload(payload, "NONE")
	require.NoError(t, err)
	assert.Equal(t, payload, out)
}

func TestDecompressGrpcPayloadZstdReaderError(t *testing.T) {
	original := newZstdReader
	newZstdReader = func(_ io.Reader, _ ...zstd.DOption) (*zstd.Decoder, error) {
		return nil, errors.New("reader error")
	}
	t.Cleanup(func() { newZstdReader = original })

	_, err := decompressGrpcPayload([]byte("payload"), "ZSTD")
	assert.Error(t, err)
}

func TestReadLengthPrefixedPayloadMaxLength(t *testing.T) {
	original := maxGrpcPayloadLength
	maxGrpcPayloadLength = 1
	t.Cleanup(func() { maxGrpcPayloadLength = original })

	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, 2)
	_, _, err := readLengthPrefixedPayload(payload)
	assert.Error(t, err)
}

func requireUint32Length(t *testing.T, n int) uint32 {
	t.Helper()
	if n < 0 || n > int(^uint32(0)) {
		t.Fatalf("payload too large: %d", n)
	}
	return uint32(n)
}

func TestArrowListDecoders(t *testing.T) {
	allocator := memory.NewGoAllocator()

	boolList := array.NewListBuilder(allocator, arrow.FixedWidthTypes.Boolean)
	boolValues, ok := boolList.ValueBuilder().(*array.BooleanBuilder)
	require.True(t, ok)
	boolList.Append(true)
	boolValues.AppendValues([]bool{true, false}, nil)
	boolArray, ok := boolList.NewArray().(*array.List)
	require.True(t, ok)
	defer boolList.Release()
	defer boolArray.Release()

	outBool, err := decodeArrowBoolList(boolArray, 0)
	assert.NoError(t, err)
	assert.Equal(t, []bool{true, false}, outBool)

	intList := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Int32)
	intValues, ok := intList.ValueBuilder().(*array.Int32Builder)
	require.True(t, ok)
	intList.Append(true)
	intValues.AppendValues([]int32{1, 2}, nil)
	intArray, ok := intList.NewArray().(*array.List)
	require.True(t, ok)
	defer intList.Release()
	defer intArray.Release()

	outInt, err := decodeArrowIntList(intArray, 0)
	assert.NoError(t, err)
	assert.Equal(t, []int{1, 2}, outInt)

	longList := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Int64)
	longValues, ok := longList.ValueBuilder().(*array.Int64Builder)
	require.True(t, ok)
	longList.Append(true)
	longValues.AppendValues([]int64{3, 4}, nil)
	longArray, ok := longList.NewArray().(*array.List)
	require.True(t, ok)
	defer longList.Release()
	defer longArray.Release()

	outLong, err := decodeArrowLongList(longArray, 0)
	assert.NoError(t, err)
	assert.Equal(t, []int64{3, 4}, outLong)

	floatList := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Float32)
	floatValues, ok := floatList.ValueBuilder().(*array.Float32Builder)
	require.True(t, ok)
	floatList.Append(true)
	floatValues.AppendValues([]float32{1.5, 2.5}, nil)
	floatArray, ok := floatList.NewArray().(*array.List)
	require.True(t, ok)
	defer floatList.Release()
	defer floatArray.Release()

	outFloat, err := decodeArrowFloatList(floatArray, 0)
	assert.NoError(t, err)
	assert.Equal(t, []float32{1.5, 2.5}, outFloat)

	doubleList := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Float64)
	doubleValues, ok := doubleList.ValueBuilder().(*array.Float64Builder)
	require.True(t, ok)
	doubleList.Append(true)
	doubleValues.AppendValues([]float64{3.5, 4.5}, nil)
	doubleArray, ok := doubleList.NewArray().(*array.List)
	require.True(t, ok)
	defer doubleList.Release()
	defer doubleArray.Release()

	outDouble, err := decodeArrowDoubleList(doubleArray, 0)
	assert.NoError(t, err)
	assert.Equal(t, []float64{3.5, 4.5}, outDouble)

	stringList := array.NewListBuilder(allocator, arrow.BinaryTypes.String)
	stringValues, ok := stringList.ValueBuilder().(*array.StringBuilder)
	require.True(t, ok)
	stringList.Append(true)
	stringValues.AppendValues([]string{"a", "b"}, nil)
	stringArray, ok := stringList.NewArray().(*array.List)
	require.True(t, ok)
	defer stringList.Release()
	defer stringArray.Release()

	outString, err := decodeArrowStringList(stringArray, 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, outString)

	nullBoolList := array.NewListBuilder(allocator, arrow.FixedWidthTypes.Boolean)
	nullBoolList.Append(false)
	nullBoolArray, ok := nullBoolList.NewArray().(*array.List)
	require.True(t, ok)
	defer nullBoolList.Release()
	defer nullBoolArray.Release()

	outBool, err = decodeArrowBoolList(nullBoolArray, 0)
	assert.NoError(t, err)
	assert.Nil(t, outBool)
}

func TestReadArrowValueNullAndDefault(t *testing.T) {
	allocator := memory.NewGoAllocator()

	boolBuilder := array.NewBooleanBuilder(allocator)
	boolBuilder.Append(true)
	boolBuilder.AppendNull()
	boolArray, ok := boolBuilder.NewArray().(*array.Boolean)
	require.True(t, ok)
	defer boolBuilder.Release()
	defer boolArray.Release()

	value, err := readArrowValue(boolArray, "BOOLEAN", 1)
	require.NoError(t, err)
	assert.Nil(t, value)

	intBuilder := array.NewInt32Builder(allocator)
	intBuilder.Append(5)
	intArray, ok := intBuilder.NewArray().(*array.Int32)
	require.True(t, ok)
	defer intBuilder.Release()
	defer intArray.Release()

	_, err = readArrowValue(intArray, "NOT_A_TYPE", 0)
	assert.Error(t, err)
}

func TestArrowListDecoderErrors(t *testing.T) {
	allocator := memory.NewGoAllocator()
	listBuilder := array.NewListBuilder(allocator, arrow.FixedWidthTypes.Boolean)
	listBuilder.Append(true)
	boolList, ok := listBuilder.NewArray().(*array.List)
	require.True(t, ok)
	defer listBuilder.Release()
	defer boolList.Release()

	intListBuilder := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Int32)
	intListBuilder.Append(true)
	intList, ok := intListBuilder.NewArray().(*array.List)
	require.True(t, ok)
	defer intListBuilder.Release()
	defer intList.Release()

	_, err := decodeArrowBoolList(intList, 0)
	assert.Error(t, err)
	_, err = decodeArrowIntList(boolList, 0)
	assert.Error(t, err)
	_, err = decodeArrowLongList(boolList, 0)
	assert.Error(t, err)
	_, err = decodeArrowFloatList(boolList, 0)
	assert.Error(t, err)
	_, err = decodeArrowDoubleList(boolList, 0)
	assert.Error(t, err)
	_, err = decodeArrowStringList(boolList, 0)
	assert.Error(t, err)
}

func TestArrowListDecoderNulls(t *testing.T) {
	allocator := memory.NewGoAllocator()

	intList := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Int32)
	intList.Append(false)
	intArray, ok := intList.NewArray().(*array.List)
	require.True(t, ok)
	defer intList.Release()
	defer intArray.Release()
	outInt, err := decodeArrowIntList(intArray, 0)
	assert.NoError(t, err)
	assert.Nil(t, outInt)

	longList := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Int64)
	longList.Append(false)
	longArray, ok := longList.NewArray().(*array.List)
	require.True(t, ok)
	defer longList.Release()
	defer longArray.Release()
	outLong, err := decodeArrowLongList(longArray, 0)
	assert.NoError(t, err)
	assert.Nil(t, outLong)

	floatList := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Float32)
	floatList.Append(false)
	floatArray, ok := floatList.NewArray().(*array.List)
	require.True(t, ok)
	defer floatList.Release()
	defer floatArray.Release()
	outFloat, err := decodeArrowFloatList(floatArray, 0)
	assert.NoError(t, err)
	assert.Nil(t, outFloat)

	doubleList := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Float64)
	doubleList.Append(false)
	doubleArray, ok := doubleList.NewArray().(*array.List)
	require.True(t, ok)
	defer doubleList.Release()
	defer doubleArray.Release()
	outDouble, err := decodeArrowDoubleList(doubleArray, 0)
	assert.NoError(t, err)
	assert.Nil(t, outDouble)

	stringList := array.NewListBuilder(allocator, arrow.BinaryTypes.String)
	stringList.Append(false)
	stringArray, ok := stringList.NewArray().(*array.List)
	require.True(t, ok)
	defer stringList.Release()
	defer stringArray.Release()
	outString, err := decodeArrowStringList(stringArray, 0)
	assert.NoError(t, err)
	assert.Nil(t, outString)
}

func TestReadArrowValueTypeMismatch(t *testing.T) {
	allocator := memory.NewGoAllocator()
	builder := array.NewStringBuilder(allocator)
	builder.Append("value")
	stringArray := builder.NewArray()
	defer builder.Release()
	defer stringArray.Release()

	_, err := readArrowValue(stringArray, "INT", 0)
	assert.Error(t, err)
}

func TestReadArrowValueTypeErrors(t *testing.T) {
	allocator := memory.NewGoAllocator()

	stringBuilder := array.NewStringBuilder(allocator)
	stringBuilder.Append("value")
	stringArray := stringBuilder.NewArray()
	defer stringBuilder.Release()
	defer stringArray.Release()

	_, err := readArrowValue(stringArray, "BOOLEAN", 0)
	assert.Error(t, err)
	_, err = readArrowValue(stringArray, "LONG", 0)
	assert.Error(t, err)
	_, err = readArrowValue(stringArray, "FLOAT", 0)
	assert.Error(t, err)
	_, err = readArrowValue(stringArray, "DOUBLE", 0)
	assert.Error(t, err)
	_, err = readArrowValue(stringArray, "MAP", 0)
	assert.Error(t, err)

	invalidMap := &bytes.Buffer{}
	require.NoError(t, binary.Write(invalidMap, binary.BigEndian, int32(-1)))
	binaryBuilder := array.NewBinaryBuilder(allocator, arrow.BinaryTypes.Binary)
	binaryBuilder.Append(invalidMap.Bytes())
	binaryArray := binaryBuilder.NewArray()
	defer binaryBuilder.Release()
	defer binaryArray.Release()
	_, err = readArrowValue(binaryArray, "MAP", 0)
	assert.Error(t, err)

	intBuilder := array.NewInt32Builder(allocator)
	intBuilder.Append(1)
	intArray := intBuilder.NewArray()
	defer intBuilder.Release()
	defer intArray.Release()

	_, err = readArrowValue(intArray, "BOOLEAN_ARRAY", 0)
	assert.Error(t, err)
	_, err = readArrowValue(intArray, "INT_ARRAY", 0)
	assert.Error(t, err)
	_, err = readArrowValue(intArray, "LONG_ARRAY", 0)
	assert.Error(t, err)
	_, err = readArrowValue(intArray, "FLOAT_ARRAY", 0)
	assert.Error(t, err)
	_, err = readArrowValue(intArray, "DOUBLE_ARRAY", 0)
	assert.Error(t, err)
	_, err = readArrowValue(intArray, "STRING_ARRAY", 0)
	assert.Error(t, err)

	_, err = readArrowValue(intArray, "STRING", 0)
	assert.Error(t, err)
	_, err = readArrowValue(intArray, "TIMESTAMP", 0)
	assert.Error(t, err)
	_, err = readArrowValue(intArray, "BYTES", 0)
	assert.Error(t, err)
	_, err = readArrowValue(intArray, "BIG_DECIMAL", 0)
	assert.Error(t, err)
	_, err = readArrowValue(intArray, "JSON", 0)
	assert.Error(t, err)
	_, err = readArrowValue(intArray, "OBJECT", 0)
	assert.Error(t, err)
}

func TestReadArrowValueSuccess(t *testing.T) {
	allocator := memory.NewGoAllocator()

	boolBuilder := array.NewBooleanBuilder(allocator)
	boolBuilder.Append(true)
	boolArray := boolBuilder.NewArray()
	defer boolBuilder.Release()
	defer boolArray.Release()
	val, err := readArrowValue(boolArray, "BOOLEAN", 0)
	assert.NoError(t, err)
	assert.Equal(t, true, val)

	intBuilder := array.NewInt32Builder(allocator)
	intBuilder.Append(1)
	intArray := intBuilder.NewArray()
	defer intBuilder.Release()
	defer intArray.Release()
	val, err = readArrowValue(intArray, "INT", 0)
	assert.NoError(t, err)
	assert.Equal(t, json.Number("1"), val)

	longBuilder := array.NewInt64Builder(allocator)
	longBuilder.Append(2)
	longArray := longBuilder.NewArray()
	defer longBuilder.Release()
	defer longArray.Release()
	val, err = readArrowValue(longArray, "LONG", 0)
	assert.NoError(t, err)
	assert.Equal(t, json.Number("2"), val)

	floatBuilder := array.NewFloat32Builder(allocator)
	floatBuilder.Append(1.25)
	floatArray := floatBuilder.NewArray()
	defer floatBuilder.Release()
	defer floatArray.Release()
	val, err = readArrowValue(floatArray, "FLOAT", 0)
	assert.NoError(t, err)
	assert.Equal(t, json.Number("1.25"), val)

	doubleBuilder := array.NewFloat64Builder(allocator)
	doubleBuilder.Append(2.5)
	doubleArray := doubleBuilder.NewArray()
	defer doubleBuilder.Release()
	defer doubleArray.Release()
	val, err = readArrowValue(doubleArray, "DOUBLE", 0)
	assert.NoError(t, err)
	assert.Equal(t, json.Number("2.5"), val)

	stringBuilder := array.NewStringBuilder(allocator)
	stringBuilder.Append("value")
	stringArray := stringBuilder.NewArray()
	defer stringBuilder.Release()
	defer stringArray.Release()
	val, err = readArrowValue(stringArray, "STRING", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	val, err = readArrowValue(stringArray, "TIMESTAMP", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	val, err = readArrowValue(stringArray, "BYTES", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	val, err = readArrowValue(stringArray, "JSON", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	val, err = readArrowValue(stringArray, "OBJECT", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	val, err = readArrowValue(stringArray, "BIG_DECIMAL", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	mapPayload := &bytes.Buffer{}
	writeInt32(t, mapPayload, 1)
	writeSchemaString(t, mapPayload, "k")
	valueBytes, err := json.Marshal("v")
	require.NoError(t, err)
	writeInt32(t, mapPayload, len(valueBytes))
	_, err = mapPayload.Write(valueBytes)
	require.NoError(t, err)

	binaryBuilder := array.NewBinaryBuilder(allocator, arrow.BinaryTypes.Binary)
	binaryBuilder.Append(mapPayload.Bytes())
	binaryArray := binaryBuilder.NewArray()
	defer binaryBuilder.Release()
	defer binaryArray.Release()
	val, err = readArrowValue(binaryArray, "MAP", 0)
	assert.NoError(t, err)
	require.IsType(t, map[string]interface{}{}, val)

	listBuilder := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Int32)
	listValues, ok := listBuilder.ValueBuilder().(*array.Int32Builder)
	require.True(t, ok)
	listBuilder.Append(true)
	listValues.AppendValues([]int32{1, 2}, nil)
	listArray, ok := listBuilder.NewArray().(*array.List)
	require.True(t, ok)
	defer listBuilder.Release()
	defer listArray.Release()
	val, err = readArrowValue(listArray, "INT_ARRAY", 0)
	assert.NoError(t, err)
	assert.Equal(t, []int{1, 2}, val)

	boolListBuilder := array.NewListBuilder(allocator, arrow.FixedWidthTypes.Boolean)
	boolListValues, ok := boolListBuilder.ValueBuilder().(*array.BooleanBuilder)
	require.True(t, ok)
	boolListBuilder.Append(true)
	boolListValues.AppendValues([]bool{true, false}, nil)
	boolListArray, ok := boolListBuilder.NewArray().(*array.List)
	require.True(t, ok)
	defer boolListBuilder.Release()
	defer boolListArray.Release()
	val, err = readArrowValue(boolListArray, "BOOLEAN_ARRAY", 0)
	assert.NoError(t, err)
	assert.Equal(t, []bool{true, false}, val)

	longListBuilder := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Int64)
	longListValues, ok := longListBuilder.ValueBuilder().(*array.Int64Builder)
	require.True(t, ok)
	longListBuilder.Append(true)
	longListValues.AppendValues([]int64{3, 4}, nil)
	longListArray, ok := longListBuilder.NewArray().(*array.List)
	require.True(t, ok)
	defer longListBuilder.Release()
	defer longListArray.Release()
	val, err = readArrowValue(longListArray, "LONG_ARRAY", 0)
	assert.NoError(t, err)
	assert.Equal(t, []int64{3, 4}, val)

	floatListBuilder := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Float32)
	floatListValues, ok := floatListBuilder.ValueBuilder().(*array.Float32Builder)
	require.True(t, ok)
	floatListBuilder.Append(true)
	floatListValues.AppendValues([]float32{1.5, 2.5}, nil)
	floatListArray, ok := floatListBuilder.NewArray().(*array.List)
	require.True(t, ok)
	defer floatListBuilder.Release()
	defer floatListArray.Release()
	val, err = readArrowValue(floatListArray, "FLOAT_ARRAY", 0)
	assert.NoError(t, err)
	assert.Equal(t, []float32{1.5, 2.5}, val)

	doubleListBuilder := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Float64)
	doubleListValues, ok := doubleListBuilder.ValueBuilder().(*array.Float64Builder)
	require.True(t, ok)
	doubleListBuilder.Append(true)
	doubleListValues.AppendValues([]float64{3.5, 4.5}, nil)
	doubleListArray, ok := doubleListBuilder.NewArray().(*array.List)
	require.True(t, ok)
	defer doubleListBuilder.Release()
	defer doubleListArray.Release()
	val, err = readArrowValue(doubleListArray, "DOUBLE_ARRAY", 0)
	assert.NoError(t, err)
	assert.Equal(t, []float64{3.5, 4.5}, val)

	stringListBuilder := array.NewListBuilder(allocator, arrow.BinaryTypes.String)
	stringListValues, ok := stringListBuilder.ValueBuilder().(*array.StringBuilder)
	require.True(t, ok)
	stringListBuilder.Append(true)
	stringListValues.AppendValues([]string{"a", "b"}, nil)
	stringListArray, ok := stringListBuilder.NewArray().(*array.List)
	require.True(t, ok)
	defer stringListBuilder.Release()
	defer stringListArray.Release()
	val, err = readArrowValue(stringListArray, "STRING_ARRAY", 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, val)

	val, err = readArrowValue(stringListArray, "TIMESTAMP_ARRAY", 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, val)

	val, err = readArrowValue(stringListArray, "BYTES_ARRAY", 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, val)

	val, err = readArrowValue(listArray, "UNKNOWN", 0)
	assert.NoError(t, err)
	assert.Nil(t, val)

	val, err = readArrowValue(stringArray, "OTHER", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value", val)
}

func TestDecodeArrowRowsError(t *testing.T) {
	allocator := memory.NewGoAllocator()
	field := arrow.Field{Name: "id", Type: arrow.BinaryTypes.String}
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	builder := array.NewStringBuilder(allocator)
	builder.Append("value")
	arrayValues := builder.NewArray()
	record := array.NewRecord(schema, []arrow.Array{arrayValues}, 1)
	defer builder.Release()
	defer arrayValues.Release()
	defer record.Release()

	buf := &bytes.Buffer{}
	writer := ipc.NewWriter(buf, ipc.WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	_, err := decodeArrowRows(buf.Bytes(), RespSchema{
		ColumnNames:     []string{"id"},
		ColumnDataTypes: []string{"LONG"},
	})
	assert.Error(t, err)
}

func TestExecuteSchemaResultTableBranch(t *testing.T) {
	schemaPayload := encodeTestSchema(t, []string{"id"}, []string{"LONG"})
	rowPayload := encodeJSONRowBlock(t, [][]interface{}{{json.Number("1")}})
	responses := []*proto.BrokerResponse{
		{Payload: []byte(`{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["id"]},"rows":[]}}`)},
		{Payload: schemaPayload},
		{
			Payload: rowPayload,
			Metadata: map[string]string{
				"rowSize":     "1",
				"encoding":    "JSON",
				"compression": "NONE",
			},
		},
	}
	server, listener, _ := startGrpcTestServer(t, responses)
	defer server.Stop()

	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "JSON",
		Compression:  "NONE",
		BlockRowSize: 1,
		Timeout:      time.Second,
	})
	require.NoError(t, err)

	resp, err := transport.execute(listener.Addr().String(), &Request{
		queryFormat: "sql",
		query:       "select * from baseballStats limit 1",
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, resp.ResultTable.GetRowCount())
}

func TestExecuteUsesConfigDefaults(t *testing.T) {
	schemaPayload := encodeTestSchema(t, []string{"id"}, []string{"LONG"})
	rowPayload := encodeJSONRowBlock(t, [][]interface{}{{json.Number("1")}})
	responses := []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: schemaPayload},
		{
			Payload: rowPayload,
			Metadata: map[string]string{
				"rowSize": "1",
			},
		},
	}
	server, listener, _ := startGrpcTestServer(t, responses)
	defer server.Stop()

	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "JSON",
		Compression:  "NONE",
		BlockRowSize: 1,
		Timeout:      time.Second,
	})
	require.NoError(t, err)

	resp, err := transport.execute(listener.Addr().String(), &Request{
		queryFormat: "sql",
		query:       "select * from baseballStats limit 1",
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, resp.ResultTable.GetRowCount())
}

func TestExecuteRowSizeZero(t *testing.T) {
	schemaPayload := encodeTestSchema(t, []string{"id"}, []string{"LONG"})
	responses := []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: schemaPayload},
		{
			Payload: []byte{},
			Metadata: map[string]string{
				"rowSize":     "0",
				"encoding":    "JSON",
				"compression": "NONE",
			},
		},
	}
	server, listener, _ := startGrpcTestServer(t, responses)
	defer server.Stop()

	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "JSON",
		Compression:  "NONE",
		BlockRowSize: 1,
		Timeout:      time.Second,
	})
	require.NoError(t, err)

	resp, err := transport.execute(listener.Addr().String(), &Request{
		queryFormat: "sql",
		query:       "select * from baseballStats limit 1",
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.ResultTable.GetRowCount())
}

func TestExecuteDecodeJSONRowsError(t *testing.T) {
	schemaPayload := encodeTestSchema(t, []string{"id"}, []string{"LONG"})
	invalidRow := &bytes.Buffer{}
	writeInt32(t, invalidRow, len([]byte("{invalid")))
	_, err := invalidRow.Write([]byte("{invalid"))
	require.NoError(t, err)

	responses := []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: schemaPayload},
		{
			Payload: invalidRow.Bytes(),
			Metadata: map[string]string{
				"rowSize":     "1",
				"encoding":    "JSON",
				"compression": "NONE",
			},
		},
	}
	server, listener, _ := startGrpcTestServer(t, responses)
	defer server.Stop()

	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "JSON",
		Compression:  "NONE",
		BlockRowSize: 1,
		Timeout:      time.Second,
	})
	require.NoError(t, err)

	_, err = transport.execute(listener.Addr().String(), &Request{
		queryFormat: "sql",
		query:       "select * from baseballStats limit 1",
	})
	assert.Error(t, err)
}

func TestExecuteDecodeArrowRowsError(t *testing.T) {
	schemaPayload := encodeTestSchema(t, []string{"id"}, []string{"LONG"})
	responses := []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: schemaPayload},
		{
			Payload: []byte("notarrow"),
			Metadata: map[string]string{
				"rowSize":     "1",
				"encoding":    "ARROW",
				"compression": "NONE",
			},
		},
	}
	server, listener, _ := startGrpcTestServer(t, responses)
	defer server.Stop()

	transport, err := newGrpcBrokerClientTransport(&GrpcConfig{
		Encoding:     "ARROW",
		Compression:  "NONE",
		BlockRowSize: 1,
		Timeout:      time.Second,
	})
	require.NoError(t, err)

	_, err = transport.execute(listener.Addr().String(), &Request{
		queryFormat: "sql",
		query:       "select * from baseballStats limit 1",
	})
	assert.Error(t, err)
}
func TestGrpcCompressionGzipSmallerThanNone(t *testing.T) {
	payload := bytes.Repeat([]byte("pinot-grpc-compression-test-"), 200)

	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, err := writer.Write(payload)
	assert.NoError(t, err)
	assert.NoError(t, writer.Close())

	compressed := buf.Bytes()
	assert.Less(t, len(compressed), len(payload))

	decompressed, err := decompressGrpcPayload(compressed, "GZIP")
	assert.NoError(t, err)
	assert.Equal(t, payload, decompressed)
}

func TestGrpcCompressionZstdSmallerThanNone(t *testing.T) {
	payload := bytes.Repeat([]byte("pinot-grpc-compression-test-"), 200)

	encoder, err := zstd.NewWriter(nil)
	assert.NoError(t, err)
	compressed := encoder.EncodeAll(payload, nil)
	assert.NoError(t, encoder.Close())

	assert.Less(t, len(compressed), len(payload))

	decompressed, err := decompressGrpcPayload(compressed, "ZSTD")
	assert.NoError(t, err)
	assert.Equal(t, payload, decompressed)
}

func encodeTestSchema(t *testing.T, columnNames []string, columnTypes []string) []byte {
	buf := &bytes.Buffer{}
	writeInt32(t, buf, len(columnNames))
	for _, name := range columnNames {
		writeSchemaString(t, buf, name)
	}
	for _, colType := range columnTypes {
		writeSchemaString(t, buf, colType)
	}
	return buf.Bytes()
}

func writeSchemaString(t *testing.T, buf *bytes.Buffer, value string) {
	writeInt32(t, buf, len(value))
	_, err := buf.Write([]byte(value))
	assert.NoError(t, err)
}

func encodeJSONRowBlock(t *testing.T, rows [][]interface{}) []byte {
	buf := &bytes.Buffer{}
	for _, row := range rows {
		rowBytes, err := json.Marshal(row)
		assert.NoError(t, err)
		writeInt32(t, buf, len(rowBytes))
		_, err = buf.Write(rowBytes)
		assert.NoError(t, err)
	}
	return buf.Bytes()
}

func encodeArrowRowBlock(t *testing.T, values []int64) []byte {
	allocator := memory.NewGoAllocator()
	field := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	builder := array.NewInt64Builder(allocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arrayValues := builder.NewArray()
	defer arrayValues.Release()

	record := array.NewRecord(schema, []arrow.Array{arrayValues}, int64(len(values)))
	defer record.Release()

	buf := &bytes.Buffer{}
	writer := ipc.NewWriter(buf, ipc.WithSchema(schema))
	err := writer.Write(record)
	assert.NoError(t, err)
	assert.NoError(t, writer.Close())
	return buf.Bytes()
}

func writeInt32(t *testing.T, buf *bytes.Buffer, value int) {
	require.LessOrEqual(t, value, math.MaxInt32)
	require.GreaterOrEqual(t, value, 0)
	// #nosec G115 -- guarded by explicit bounds checks above.
	err := binary.Write(buf, binary.BigEndian, int32(value))
	assert.NoError(t, err)
}
