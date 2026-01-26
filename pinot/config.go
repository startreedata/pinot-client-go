package pinot

import "time"

// ClientConfig configs to create a PinotDbConnection
type ClientConfig struct {
	// Additional HTTP headers to include in broker query API requests
	ExtraHTTPHeader map[string]string
	// GrpcConfig enables gRPC broker queries when set
	GrpcConfig *GrpcConfig
	// Zookeeper Configs
	ZkConfig *ZookeeperConfig
	// Controller Config
	ControllerConfig *ControllerConfig
	// BrokerList
	BrokerList []string
	// HTTP request timeout in your broker query for API requests
	HTTPTimeout time.Duration
	// UseMultistageEngine is a flag to enable multistage query execution engine
	UseMultistageEngine bool
}

// GrpcConfig describes how to configure broker gRPC queries
type GrpcConfig struct {
	// Encoding controls result serialization. Supported values: JSON, ARROW.
	Encoding string
	// Compression controls response compression. Supported values: ZSTD, LZ4_FAST, LZ4_HIGH, DEFLATE, GZIP, SNAPPY, NONE.
	Compression string
	// BlockRowSize is the number of rows per response block.
	BlockRowSize int
	// Timeout controls gRPC request timeout.
	Timeout time.Duration
	// ExtraMetadata adds metadata entries to the gRPC request.
	ExtraMetadata map[string]string
	// TLS config for secure gRPC connections.
	TLSConfig *GrpcTLSConfig
}

// GrpcTLSConfig configures TLS for gRPC connections.
type GrpcTLSConfig struct {
	Enabled            bool
	CACertPath         string
	ServerName         string
	InsecureSkipVerify bool
}

// ZookeeperConfig describes how to config Pinot Zookeeper connection
type ZookeeperConfig struct {
	PathPrefix        string
	ZookeeperPath     []string
	SessionTimeoutSec int
}

// ControllerConfig describes connection of a controller-based selector that
// periodically fetches table-to-broker mapping via the controller API
type ControllerConfig struct {
	// Additional HTTP headers to include in the controller API request
	ExtraControllerAPIHeaders map[string]string
	ControllerAddress         string
	// Frequency of broker data refresh in milliseconds via controller API - defaults to 1000ms
	UpdateFreqMs int
}
