package pinot

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	proto "github.com/startreedata/pinot-client-go/pinot/proto"
)

// Benchmarks for the gRPC broker transport against an in-process broker on
// loopback. They measure client-side overhead per query (connection setup,
// request encoding, response decoding) for a small result, which is the
// common case for user-facing analytics.
//
// Run with:
//
//	go test ./pinot -run '^$' -bench BenchmarkGrpcBrokerTransport -benchmem -count 5

// benchLengthPrefixed concatenates each part as a big-endian int32 length
// followed by the bytes, the framing used by Pinot's gRPC schema and JSON row
// blocks.
func benchLengthPrefixed(parts ...[]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = binary.BigEndian.AppendUint32(out, uint32(len(part))) // #nosec G115 -- small test payloads.
		out = append(out, part...)
	}
	return out
}

func benchGrpcResponses() []*proto.BrokerResponse {
	// Schema block: column count, then column names, then column types.
	schema := binary.BigEndian.AppendUint32(nil, 1)
	schema = append(schema, benchLengthPrefixed([]byte("id"), []byte("LONG"))...)
	return []*proto.BrokerResponse{
		{Payload: []byte(`{"exceptions":[]}`)},
		{Payload: schema},
		{
			Payload: benchLengthPrefixed([]byte("[1]"), []byte("[2]")),
			Metadata: map[string]string{
				"rowSize":     "2",
				"encoding":    "JSON",
				"compression": "NONE",
			},
		},
	}
}

func startBenchGrpcServer(tb testing.TB, opts ...grpc.ServerOption) string {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	server := grpc.NewServer(opts...)
	proto.RegisterPinotQueryBrokerServer(server, &mockPinotQueryBrokerServer{responses: benchGrpcResponses()})
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			tb.Error(serveErr)
		}
	}()
	tb.Cleanup(server.Stop)
	return listener.Addr().String()
}

// writeBenchTLSCert creates a self-signed certificate for 127.0.0.1, writes the
// PEM to dir/ca.pem and returns the server certificate and the PEM path.
func writeBenchTLSCert(tb testing.TB, dir string) (tls.Certificate, string) {
	tb.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		tb.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		tb.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		tb.Fatal(err)
	}
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		tb.Fatal(err)
	}
	return cert, caPath
}

func runGrpcTransportBenchmark(b *testing.B, config *GrpcConfig, address string, parallel bool) {
	transport, err := newGrpcBrokerClientTransport(config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { closeIfCloser(transport) })
	request := &Request{queryFormat: "sql", query: "select id from baseballStats limit 2"}
	// One warm-up query so a reused connection is already established.
	if _, err := transport.execute(address, request); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := transport.execute(address, request); err != nil {
					b.Error(err)
					return
				}
			}
		})
		return
	}
	for i := 0; i < b.N; i++ {
		if _, err := transport.execute(address, request); err != nil {
			b.Fatal(err)
		}
	}
}

func closeIfCloser(v interface{}) {
	if c, ok := v.(interface{ Close() error }); ok {
		if closeErr := c.Close(); closeErr != nil {
			_ = closeErr
		}
	}
}

func BenchmarkGrpcBrokerTransport(b *testing.B) {
	baseConfig := func() *GrpcConfig {
		return &GrpcConfig{Encoding: "JSON", Compression: "NONE", BlockRowSize: 2, Timeout: 5 * time.Second}
	}

	plainAddress := startBenchGrpcServer(b)
	b.Run("plaintext/serial", func(b *testing.B) {
		runGrpcTransportBenchmark(b, baseConfig(), plainAddress, false)
	})
	b.Run("plaintext/parallel", func(b *testing.B) {
		runGrpcTransportBenchmark(b, baseConfig(), plainAddress, true)
	})

	cert, caPath := writeBenchTLSCert(b, b.TempDir())
	tlsAddress := startBenchGrpcServer(b, grpc.Creds(credentials.NewServerTLSFromCert(&cert)))
	tlsConfig := func() *GrpcConfig {
		config := baseConfig()
		config.TLSConfig = &GrpcTLSConfig{Enabled: true, CACertPath: caPath}
		return config
	}
	b.Run("tls/serial", func(b *testing.B) {
		runGrpcTransportBenchmark(b, tlsConfig(), tlsAddress, false)
	})
	b.Run("tls/parallel", func(b *testing.B) {
		runGrpcTransportBenchmark(b, tlsConfig(), tlsAddress, true)
	})
}
