package pinot

import "time"

// ClientConfig configs to create a PinotDbConnection
type ClientConfig struct {
	// Additional HTTP headers to include in broker query API requests
	ExtraHTTPHeader map[string]string
	// Zookeeper Configs
	ZkConfig *ZookeeperConfig
	// Controller Config
	ControllerConfig *ControllerConfig
	// BrokerList
	BrokerList []string
	// HTTP request timeout in your broker query for API requests
	HTTPTimeout time.Duration
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
