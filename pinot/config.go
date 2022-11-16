package pinot

import "time"

// ClientConfig configs to create a PinotDbConnection
type ClientConfig struct {

	// Additional HTTP headers to include in broker query API requests
	ExtraHTTPHeader map[string]string

	// HTTP request timeout in your broker query for API requests
	HTTPTimeout time.Duration

	// Zookeeper Configs
	ZkConfig *ZookeeperConfig

	// BrokerList
	BrokerList []string

	// Controller Config
	ControllerConfig *ControllerConfig
}

// ZookeeperConfig describes how to config Pinot Zookeeper connection
type ZookeeperConfig struct {
	ZookeeperPath     []string
	PathPrefix        string
	SessionTimeoutSec int
}

// ControllerConfig describes connection of a controller-based selector that
// periodically fetches table-to-broker mapping via the controller API
type ControllerConfig struct {
	ControllerAddress string
	// Frequency of broker data refresh in milliseconds via controller API - defaults to 1000ms
	UpdateFreqMs int
	// Additional HTTP headers to include in the controller API request
	ExtraControllerAPIHeaders map[string]string
}
