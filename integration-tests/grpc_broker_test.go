package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/startreedata/pinot-client-go/pinot"
	proto "github.com/startreedata/pinot-client-go/pinot/proto"
)

var (
	grpcBrokerHost = getEnv("BROKER_GRPC_HOST", "127.0.0.1")
	grpcBrokerPort = getEnv("BROKER_GRPC_PORT", "8010")
)

func TestGrpcBrokerQuery(t *testing.T) {
	requireGrpcAvailable(t)

	resp, err := executeGrpcQueryWithQuery(t, "NONE", "select count(*) as cnt from baseballStats limit 1")
	require.NoError(t, err)
	assert.Equal(t, int64(97889), resp.ResultTable.GetLong(0, 0))
}

func TestGrpcBrokerQueryWithCompression(t *testing.T) {
	requireGrpcAvailable(t)

	compressions := []string{"NONE", "GZIP", "ZSTD", "SNAPPY", "LZ4", "LZ4_FAST", "LZ4_HIGH", "DEFLATE"}
	for _, compression := range compressions {
		t.Run(compression, func(t *testing.T) {
			resp, err := executeGrpcQueryWithQuery(t, compression, "select count(*) as cnt from baseballStats limit 1")
			require.NoError(t, err)
			assert.Equal(t, int64(97889), resp.ResultTable.GetLong(0, 0))
		})
	}
}

func TestGrpcBrokerQueryCompressionSize(t *testing.T) {
	requireGrpcAvailable(t)

	query := "select * from baseballStats limit 100000"
	noneSize, noneRows, err := grpcPayloadSizeAndRowCount(t, query, "NONE")
	require.NoError(t, err)
	fmt.Printf("grpc payload size [%s]: %d bytes\n", "NONE", noneSize)
	assert.Equal(t, 97889, noneRows)
	compressions := []string{"GZIP", "ZSTD", "SNAPPY", "LZ4", "LZ4_HIGH", "LZ4_FAST", "DEFLATE"}
	for _, compression := range compressions {
		t.Run(compression, func(t *testing.T) {
			compressedSize, rowCount, sizeErr := grpcPayloadSizeAndRowCount(t, query, compression)
			require.NoError(t, sizeErr)
			fmt.Printf("grpc payload size [%s]: %d bytes\n", compression, compressedSize)
			assert.Less(t, compressedSize, noneSize)
			assert.Equal(t, 97889, rowCount)
		})
	}
}

func executeGrpcQueryWithQuery(t *testing.T, compression string, query string) (*pinot.BrokerResponse, error) {
	t.Helper()

	client, err := pinot.NewWithConfig(&pinot.ClientConfig{
		BrokerList: []string{grpcBrokerHost + ":" + grpcBrokerPort},
		GrpcConfig: &pinot.GrpcConfig{
			Encoding:     "JSON",
			Compression:  compression,
			BlockRowSize: 1,
			Timeout:      10 * time.Second,
		},
	})
	if err != nil {
		return nil, err
	}

	client.UseMultistageEngine(true)
	var resp *pinot.BrokerResponse
	for i := 0; i < 10; i++ {
		resp, err = client.ExecuteSQL("baseballStats", query)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.ResultTable == nil {
		return nil, fmt.Errorf("missing response data")
	}

	return resp, nil
}

func grpcPayloadSizeAndRowCount(t *testing.T, query string, compression string) (int, int, error) {
	t.Helper()

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		total, rows, err := grpcPayloadSizeOnce(t, query, compression)
		if err == nil {
			return total, rows, nil
		}
		lastErr = err
		time.Sleep(2 * time.Second)
	}

	return 0, 0, lastErr
}

func grpcPayloadSizeOnce(t *testing.T, query string, compression string) (int, int, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	//nolint:staticcheck // grpc.DialContext is still supported for client setup.
	conn, err := grpc.DialContext(ctx, grpcBrokerHost+":"+grpcBrokerPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			_ = closeErr
		}
	}()

	client := proto.NewPinotQueryBrokerClient(conn)
	req := &proto.BrokerRequest{
		Sql: query,
		Metadata: map[string]string{
			"blockRowSize": "10000",
			"encoding":     "JSON",
			"compression":  compression,
			"queryOptions": fmt.Sprintf("groupByMode=sql;responseFormat=sql;useMultistageEngine=true;timeoutMs=%d", int64(60*time.Second/time.Millisecond)),
		},
	}

	stream, err := client.Submit(ctx, req)
	if err != nil {
		return 0, 0, err
	}

	total := 0
	rowCount := 0
	for {
		resp, recvErr := stream.Recv()
		if recvErr == io.EOF {
			return total, rowCount, nil
		}
		if recvErr != nil {
			return 0, 0, recvErr
		}
		total += len(resp.Payload)
		blockRows, err := decodeRowCount(resp, compression)
		if err != nil {
			return 0, 0, err
		}
		rowCount += blockRows
	}
}

func requireGrpcAvailable(t *testing.T) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", grpcBrokerHost+":"+grpcBrokerPort, 2*time.Second)
	if err != nil {
		t.Skipf("grpc broker not reachable at %s:%s: %v", grpcBrokerHost, grpcBrokerPort, err)
		return
	}
	if closeErr := conn.Close(); closeErr != nil {
		_ = closeErr
	}
}

func decodeRowCount(resp *proto.BrokerResponse, compression string) (int, error) {
	if resp == nil || resp.Metadata == nil {
		return 0, nil
	}
	rowSizeValue, ok := resp.Metadata["rowSize"]
	if !ok || rowSizeValue == "" {
		return 0, nil
	}
	rowSize, err := strconv.Atoi(rowSizeValue)
	if err != nil {
		return 0, err
	}
	if rowSize == 0 {
		return 0, nil
	}
	payload, err := decompressPayload(resp.Payload, compression)
	if err != nil {
		return 0, err
	}
	return decodeJSONRowCount(payload, rowSize)
}

func decodeJSONRowCount(payload []byte, rowSize int) (int, error) {
	reader := bytes.NewReader(payload)
	count := 0
	for i := 0; i < rowSize; i++ {
		var rowLength int32
		if err := binary.Read(reader, binary.BigEndian, &rowLength); err != nil {
			return 0, err
		}
		if rowLength < 0 {
			return 0, fmt.Errorf("invalid row length: %d", rowLength)
		}
		rowBytes := make([]byte, rowLength)
		if _, err := io.ReadFull(reader, rowBytes); err != nil {
			return 0, err
		}
		var row []interface{}
		decoder := json.NewDecoder(bytes.NewReader(rowBytes))
		decoder.UseNumber()
		if err := decoder.Decode(&row); err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func decompressPayload(payload []byte, compression string) ([]byte, error) {
	return pinot.DecompressGrpcPayload(payload, compression)
}
