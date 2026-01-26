package pinot

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	proto "github.com/startreedata/pinot-client-go/pinot/proto"
)

const (
	defaultGrpcBlockRowSize = 10000
	defaultGrpcCompression  = "ZSTD"
	defaultGrpcEncoding     = "JSON"
)

var newZstdReader = zstd.NewReader

//nolint:staticcheck // grpc.DialContext is still supported by gRPC for 1.x.
var grpcDialContext = grpc.DialContext
var maxGrpcPayloadLength = int(^uint(0) >> 1)

type grpcBrokerClientTransport struct {
	config *GrpcConfig
}

func newGrpcBrokerClientTransport(config *GrpcConfig) (*grpcBrokerClientTransport, error) {
	if config == nil {
		return nil, fmt.Errorf("grpc config is required")
	}
	return &grpcBrokerClientTransport{
		config: config,
	}, nil
}

func (t *grpcBrokerClientTransport) execute(brokerAddress string, query *Request) (*BrokerResponse, error) {
	address := normalizeGrpcAddress(brokerAddress)
	ctx := context.Background()
	if t.config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.config.Timeout)
		defer cancel()
	}
	dialOptions, err := buildGrpcDialOptions(t.config)
	if err != nil {
		return nil, err
	}
	//nolint:staticcheck // grpc.NewClient lacks context-based timeout semantics here.
	conn, err := grpcDialContext(ctx, address, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial grpc broker %s: %w", address, err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			_ = closeErr
		}
	}()

	client := proto.NewPinotQueryBrokerClient(conn)
	request := &proto.BrokerRequest{
		Sql:      query.query,
		Metadata: buildGrpcMetadata(t.config, query),
	}
	stream, err := client.Submit(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("grpc submit failed: %w", err)
	}

	var response *BrokerResponse
	var schema *RespSchema
	for {
		block, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			return nil, fmt.Errorf("grpc response error: %w", recvErr)
		}
		if response == nil {
			var brokerResponse BrokerResponse
			if decodeErr := decodeJSONWithNumber(block.Payload, &brokerResponse); decodeErr != nil {
				return nil, fmt.Errorf("failed to decode grpc metadata block: %w", decodeErr)
			}
			response = &brokerResponse
			continue
		}
		if schema == nil {
			decodedSchema, schemaErr := decodeDataSchema(block.Payload)
			if schemaErr != nil {
				return nil, fmt.Errorf("failed to decode grpc schema block: %w", schemaErr)
			}
			schema = &decodedSchema
			if response.ResultTable == nil {
				response.ResultTable = &ResultTable{
					DataSchema: *schema,
					Rows:       [][]interface{}{},
				}
			} else {
				response.ResultTable.DataSchema = *schema
			}
			continue
		}

		rowSize, err := parseRowSize(block.Metadata)
		if err != nil {
			return nil, err
		}
		encoding := normalizeAlgorithm(block.Metadata["encoding"], t.config.Encoding, defaultGrpcEncoding)
		compression := normalizeAlgorithm(block.Metadata["compression"], t.config.Compression, defaultGrpcCompression)
		payload, err := decompressGrpcPayload(block.Payload, compression)
		if err != nil {
			return nil, err
		}

		var rows [][]interface{}
		switch strings.ToUpper(encoding) {
		case "JSON":
			decodedRows, decodeErr := decodeJSONRows(payload, rowSize)
			if decodeErr != nil {
				return nil, decodeErr
			}
			rows = decodedRows
		case "ARROW":
			decodedRows, decodeErr := decodeArrowRows(payload, *schema)
			if decodeErr != nil {
				return nil, decodeErr
			}
			rows = decodedRows
		default:
			return nil, fmt.Errorf("unsupported grpc encoding: %s", encoding)
		}
		response.ResultTable.Rows = append(response.ResultTable.Rows, rows...)
	}
	if response == nil {
		return nil, fmt.Errorf("no grpc response payload received")
	}
	return response, nil
}

func normalizeGrpcAddress(address string) string {
	trimmed := strings.TrimPrefix(address, "grpc://")
	trimmed = strings.TrimPrefix(trimmed, "grpcs://")
	return trimmed
}

func buildGrpcMetadata(config *GrpcConfig, query *Request) map[string]string {
	metadata := map[string]string{}
	for k, v := range config.ExtraMetadata {
		metadata[k] = v
	}
	blockRowSize := config.BlockRowSize
	if blockRowSize <= 0 {
		blockRowSize = defaultGrpcBlockRowSize
	}
	metadata["blockRowSize"] = strconv.Itoa(blockRowSize)

	encoding := normalizeAlgorithm(config.Encoding, "", defaultGrpcEncoding)
	metadata["encoding"] = strings.ToUpper(encoding)

	compression := normalizeAlgorithm(config.Compression, "", defaultGrpcCompression)
	metadata["compression"] = strings.ToUpper(compression)

	queryOptions := buildGrpcQueryOptions(query, config.Timeout)
	if queryOptions != "" {
		metadata["queryOptions"] = queryOptions
	}
	if query.trace {
		metadata["trace"] = "true"
	}
	return metadata
}

func buildGrpcQueryOptions(query *Request, timeout time.Duration) string {
	queryOptions := ""
	if query.queryFormat == "sql" {
		queryOptions = "groupByMode=sql;responseFormat=sql"
	}
	if query.useMultistageEngine {
		if queryOptions != "" {
			queryOptions += ";"
		}
		queryOptions += "useMultistageEngine=true"
	}
	if timeout > 0 {
		if queryOptions != "" {
			queryOptions += ";"
		}
		queryOptions += fmt.Sprintf("timeoutMs=%d", timeout.Milliseconds())
	}
	return queryOptions
}

func normalizeAlgorithm(primary string, fallback string, defaultValue string) string {
	if primary != "" {
		return primary
	}
	if fallback != "" {
		return fallback
	}
	return defaultValue
}

func buildGrpcDialOptions(config *GrpcConfig) ([]grpc.DialOption, error) {
	var creds credentials.TransportCredentials
	if config.TLSConfig != nil && config.TLSConfig.Enabled {
		tlsConfig := &tls.Config{
			// #nosec G402 -- allow opt-in for environments with self-signed certs.
			InsecureSkipVerify: config.TLSConfig.InsecureSkipVerify,
			ServerName:         config.TLSConfig.ServerName,
		}
		if config.TLSConfig.CACertPath != "" {
			caBytes, err := os.ReadFile(config.TLSConfig.CACertPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read grpc CA cert: %w", err)
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(caBytes) {
				return nil, fmt.Errorf("failed to parse grpc CA cert")
			}
			tlsConfig.RootCAs = certPool
		}
		creds = credentials.NewTLS(tlsConfig)
	} else {
		creds = insecure.NewCredentials()
	}
	return []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}, nil
}

func parseRowSize(metadata map[string]string) (int, error) {
	if metadata == nil {
		return 0, fmt.Errorf("grpc response metadata missing")
	}
	rowSizeValue, ok := metadata["rowSize"]
	if !ok {
		return 0, fmt.Errorf("grpc response metadata missing rowSize")
	}
	rowSize, err := strconv.Atoi(rowSizeValue)
	if err != nil {
		return 0, fmt.Errorf("invalid grpc rowSize %q: %w", rowSizeValue, err)
	}
	return rowSize, nil
}

func decodeDataSchema(payload []byte) (RespSchema, error) {
	reader := bytes.NewReader(payload)
	var columnCount int32
	if err := binary.Read(reader, binary.BigEndian, &columnCount); err != nil {
		return RespSchema{}, fmt.Errorf("failed to read schema column count: %w", err)
	}
	if columnCount < 0 {
		return RespSchema{}, fmt.Errorf("invalid schema column count: %d", columnCount)
	}
	columnNames := make([]string, columnCount)
	columnTypes := make([]string, columnCount)
	for i := int32(0); i < columnCount; i++ {
		name, err := readSchemaString(reader)
		if err != nil {
			return RespSchema{}, fmt.Errorf("failed to read schema column name: %w", err)
		}
		columnNames[i] = name
	}
	for i := int32(0); i < columnCount; i++ {
		colType, err := readSchemaString(reader)
		if err != nil {
			return RespSchema{}, fmt.Errorf("failed to read schema column type: %w", err)
		}
		columnTypes[i] = colType
	}
	return RespSchema{
		ColumnNames:     columnNames,
		ColumnDataTypes: columnTypes,
	}, nil
}

func readSchemaString(reader *bytes.Reader) (string, error) {
	var length int32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return "", err
	}
	if length < 0 {
		return "", fmt.Errorf("invalid schema string length: %d", length)
	}
	bytesValue := make([]byte, length)
	if _, err := io.ReadFull(reader, bytesValue); err != nil {
		return "", err
	}
	return string(bytesValue), nil
}

func decodeJSONRows(payload []byte, rowSize int) ([][]interface{}, error) {
	if rowSize == 0 {
		return [][]interface{}{}, nil
	}
	reader := bytes.NewReader(payload)
	rows := make([][]interface{}, 0, rowSize)
	for i := 0; i < rowSize; i++ {
		var rowLength int32
		if err := binary.Read(reader, binary.BigEndian, &rowLength); err != nil {
			return nil, fmt.Errorf("failed to read row length: %w", err)
		}
		if rowLength < 0 {
			return nil, fmt.Errorf("invalid row length: %d", rowLength)
		}
		rowBytes := make([]byte, rowLength)
		if _, err := io.ReadFull(reader, rowBytes); err != nil {
			return nil, fmt.Errorf("failed to read row bytes: %w", err)
		}
		var row []interface{}
		decoder := json.NewDecoder(bytes.NewReader(rowBytes))
		decoder.UseNumber()
		if err := decoder.Decode(&row); err != nil {
			return nil, fmt.Errorf("failed to decode row json: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func decodeArrowRows(payload []byte, schema RespSchema) ([][]interface{}, error) {
	allocator := memory.NewGoAllocator()
	reader, err := ipc.NewReader(bytes.NewReader(payload), ipc.WithAllocator(allocator))
	if err != nil {
		return nil, fmt.Errorf("failed to read arrow payload: %w", err)
	}
	defer reader.Release()

	rows := [][]interface{}{}
	for reader.Next() {
		record := reader.Record()
		numRows := int(record.NumRows())
		numCols := int(record.NumCols())
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			row := make([]interface{}, numCols)
			for colIdx := 0; colIdx < numCols; colIdx++ {
				colType := schema.ColumnDataTypes[colIdx]
				value, err := readArrowValue(record.Column(colIdx), colType, rowIdx)
				if err != nil {
					return nil, fmt.Errorf("failed to read arrow value: %w", err)
				}
				row[colIdx] = value
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func readArrowValue(column arrow.Array, columnType string, rowIdx int) (interface{}, error) {
	if column.IsNull(rowIdx) {
		return nil, nil
	}
	switch strings.ToUpper(columnType) {
	case "BOOLEAN":
		boolCol, ok := column.(*array.Boolean)
		if !ok {
			return nil, fmt.Errorf("expected BOOLEAN column")
		}
		return boolCol.Value(rowIdx), nil
	case "INT":
		intCol, ok := column.(*array.Int32)
		if !ok {
			return nil, fmt.Errorf("expected INT column")
		}
		return json.Number(strconv.FormatInt(int64(intCol.Value(rowIdx)), 10)), nil
	case "LONG":
		intCol, ok := column.(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("expected LONG column")
		}
		return json.Number(strconv.FormatInt(intCol.Value(rowIdx), 10)), nil
	case "FLOAT":
		floatCol, ok := column.(*array.Float32)
		if !ok {
			return nil, fmt.Errorf("expected FLOAT column")
		}
		return json.Number(strconv.FormatFloat(float64(floatCol.Value(rowIdx)), 'f', -1, 32)), nil
	case "DOUBLE":
		floatCol, ok := column.(*array.Float64)
		if !ok {
			return nil, fmt.Errorf("expected DOUBLE column")
		}
		return json.Number(strconv.FormatFloat(floatCol.Value(rowIdx), 'f', -1, 64)), nil
	case "TIMESTAMP", "STRING", "BYTES", "BIG_DECIMAL", "JSON", "OBJECT":
		stringCol, ok := column.(*array.String)
		if !ok {
			return nil, fmt.Errorf("expected STRING column")
		}
		return stringCol.Value(rowIdx), nil
	case "MAP":
		binaryCol, ok := column.(*array.Binary)
		if !ok {
			return nil, fmt.Errorf("expected MAP column")
		}
		return decodeMap(binaryCol.Value(rowIdx))
	case "UNKNOWN":
		return nil, nil
	case "BOOLEAN_ARRAY":
		listCol, ok := column.(*array.List)
		if !ok {
			return nil, fmt.Errorf("expected BOOLEAN_ARRAY column")
		}
		return decodeArrowBoolList(listCol, rowIdx)
	case "INT_ARRAY":
		listCol, ok := column.(*array.List)
		if !ok {
			return nil, fmt.Errorf("expected INT_ARRAY column")
		}
		return decodeArrowIntList(listCol, rowIdx)
	case "LONG_ARRAY":
		listCol, ok := column.(*array.List)
		if !ok {
			return nil, fmt.Errorf("expected LONG_ARRAY column")
		}
		return decodeArrowLongList(listCol, rowIdx)
	case "FLOAT_ARRAY":
		listCol, ok := column.(*array.List)
		if !ok {
			return nil, fmt.Errorf("expected FLOAT_ARRAY column")
		}
		return decodeArrowFloatList(listCol, rowIdx)
	case "DOUBLE_ARRAY":
		listCol, ok := column.(*array.List)
		if !ok {
			return nil, fmt.Errorf("expected DOUBLE_ARRAY column")
		}
		return decodeArrowDoubleList(listCol, rowIdx)
	case "TIMESTAMP_ARRAY", "STRING_ARRAY", "BYTES_ARRAY":
		listCol, ok := column.(*array.List)
		if !ok {
			return nil, fmt.Errorf("expected STRING_ARRAY column")
		}
		return decodeArrowStringList(listCol, rowIdx)
	default:
		stringCol, ok := column.(*array.String)
		if !ok {
			return nil, fmt.Errorf("unexpected column type %T", column)
		}
		return stringCol.Value(rowIdx), nil
	}
}

func decodeArrowBoolList(list *array.List, rowIdx int) ([]bool, error) {
	if list.IsNull(rowIdx) {
		return nil, nil
	}
	start, end := list.ValueOffsets(rowIdx)
	values, ok := list.ListValues().(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("expected BOOLEAN list values")
	}
	length := int(end - start)
	output := make([]bool, length)
	for i := int(start); i < int(end); i++ {
		output[i-int(start)] = values.Value(i)
	}
	return output, nil
}

func decodeArrowIntList(list *array.List, rowIdx int) ([]int, error) {
	if list.IsNull(rowIdx) {
		return nil, nil
	}
	start, end := list.ValueOffsets(rowIdx)
	values, ok := list.ListValues().(*array.Int32)
	if !ok {
		return nil, fmt.Errorf("expected INT list values")
	}
	length := int(end - start)
	output := make([]int, length)
	for i := int(start); i < int(end); i++ {
		output[i-int(start)] = int(values.Value(i))
	}
	return output, nil
}

func decodeArrowLongList(list *array.List, rowIdx int) ([]int64, error) {
	if list.IsNull(rowIdx) {
		return nil, nil
	}
	start, end := list.ValueOffsets(rowIdx)
	values, ok := list.ListValues().(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("expected LONG list values")
	}
	length := int(end - start)
	output := make([]int64, length)
	for i := int(start); i < int(end); i++ {
		output[i-int(start)] = values.Value(i)
	}
	return output, nil
}

func decodeArrowFloatList(list *array.List, rowIdx int) ([]float32, error) {
	if list.IsNull(rowIdx) {
		return nil, nil
	}
	start, end := list.ValueOffsets(rowIdx)
	values, ok := list.ListValues().(*array.Float32)
	if !ok {
		return nil, fmt.Errorf("expected FLOAT list values")
	}
	length := int(end - start)
	output := make([]float32, length)
	for i := int(start); i < int(end); i++ {
		output[i-int(start)] = values.Value(i)
	}
	return output, nil
}

func decodeArrowDoubleList(list *array.List, rowIdx int) ([]float64, error) {
	if list.IsNull(rowIdx) {
		return nil, nil
	}
	start, end := list.ValueOffsets(rowIdx)
	values, ok := list.ListValues().(*array.Float64)
	if !ok {
		return nil, fmt.Errorf("expected DOUBLE list values")
	}
	length := int(end - start)
	output := make([]float64, length)
	for i := int(start); i < int(end); i++ {
		output[i-int(start)] = values.Value(i)
	}
	return output, nil
}

func decodeArrowStringList(list *array.List, rowIdx int) ([]string, error) {
	if list.IsNull(rowIdx) {
		return nil, nil
	}
	start, end := list.ValueOffsets(rowIdx)
	values, ok := list.ListValues().(*array.String)
	if !ok {
		return nil, fmt.Errorf("expected STRING list values")
	}
	length := int(end - start)
	output := make([]string, length)
	for i := int(start); i < int(end); i++ {
		output[i-int(start)] = values.Value(i)
	}
	return output, nil
}

func decodeMap(payload []byte) (map[string]interface{}, error) {
	reader := bytes.NewReader(payload)
	var size int32
	if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	if size == 0 {
		return map[string]interface{}{}, nil
	}
	if size < 0 {
		return nil, fmt.Errorf("invalid map size: %d", size)
	}
	output := make(map[string]interface{}, size)
	for i := int32(0); i < size; i++ {
		key, err := readSchemaString(reader)
		if err != nil {
			return nil, err
		}
		var valueLength int32
		if err := binary.Read(reader, binary.BigEndian, &valueLength); err != nil {
			return nil, err
		}
		if valueLength < 0 {
			return nil, fmt.Errorf("invalid map value length: %d", valueLength)
		}
		valueBytes := make([]byte, valueLength)
		if _, err := io.ReadFull(reader, valueBytes); err != nil {
			return nil, err
		}
		var value interface{}
		decoder := json.NewDecoder(bytes.NewReader(valueBytes))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		output[key] = value
	}
	return output, nil
}

func decompressGrpcPayload(payload []byte, compression string) ([]byte, error) {
	switch strings.ToUpper(compression) {
	case "PASS_THROUGH", "NONE", "":
		return payload, nil
	case "ZSTD", "ZSTANDARD":
		output, err := decodeZstdPayload(payload)
		if err == nil {
			return output, nil
		}
		expectedLength, compressed, prefixErr := readLengthPrefixedPayload(payload)
		if prefixErr != nil {
			return nil, err
		}
		output, prefixErr = decodeZstdPayload(compressed)
		if prefixErr != nil {
			return nil, fmt.Errorf("zstd decompress failed: frame=%v, length-prefixed=%v", err, prefixErr)
		}
		if expectedLength != len(output) {
			return nil, fmt.Errorf("zstd length prefix mismatch: expected %d, got %d", expectedLength, len(output))
		}
		return output, nil
	case "LZ4", "LZ4_FAST", "LZ4_HIGH":
		reader := lz4.NewReader(bytes.NewReader(payload))
		output, err := io.ReadAll(reader)
		if err == nil {
			return output, nil
		}
		expectedLength, compressed, prefixErr := readLengthPrefixedPayload(payload)
		if prefixErr != nil {
			return nil, err
		}
		if expectedLength == 0 {
			return []byte{}, nil
		}
		output = make([]byte, expectedLength)
		n, prefixErr := lz4.UncompressBlock(compressed, output)
		if prefixErr != nil {
			return nil, fmt.Errorf("lz4 decompress failed: frame=%v, length-prefixed=%v", err, prefixErr)
		}
		if n != expectedLength {
			return nil, fmt.Errorf("lz4 length prefix mismatch: expected %d, got %d", expectedLength, n)
		}
		return output, nil
	case "DEFLATE":
		reader, err := zlib.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		defer func() {
			if closeErr := reader.Close(); closeErr != nil {
				_ = closeErr
			}
		}()
		return io.ReadAll(reader)
	case "GZIP":
		reader, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		defer func() {
			if closeErr := reader.Close(); closeErr != nil {
				_ = closeErr
			}
		}()
		return io.ReadAll(reader)
	case "SNAPPY":
		return snappy.Decode(nil, payload)
	default:
		return nil, fmt.Errorf("unsupported grpc compression: %s", compression)
	}
}

// DecompressGrpcPayload exposes gRPC payload decompression for integration tooling.
func DecompressGrpcPayload(payload []byte, compression string) ([]byte, error) {
	return decompressGrpcPayload(payload, compression)
}

func decodeZstdPayload(payload []byte) ([]byte, error) {
	decoder, err := newZstdReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	return decoder.DecodeAll(payload, nil)
}

func readLengthPrefixedPayload(payload []byte) (int, []byte, error) {
	if len(payload) < 4 {
		return 0, nil, fmt.Errorf("invalid length-prefixed payload: %d bytes", len(payload))
	}
	length := binary.BigEndian.Uint32(payload[:4])
	if int64(length) > int64(maxGrpcPayloadLength) {
		return 0, nil, fmt.Errorf("invalid length prefix: %d", length)
	}
	return int(length), payload[4:], nil
}
