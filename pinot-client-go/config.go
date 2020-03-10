package pinot

// ClientConfig configs to create a PinotDbConnection
type ClientConfig struct {

	// Request header
	ExtraHTTPHeader map[string]string

	// Zookeeper Configs
	ZkConfig *ZookeeperConfig

	// BrokerList
	BrokerList []string
}

// ZookeeperConfig describes how to config Pinot Zookeeper connection
type ZookeeperConfig struct {
	ZookeeperPath     []string
	PathPrefix        string
	SessionTimeoutSec int
}
