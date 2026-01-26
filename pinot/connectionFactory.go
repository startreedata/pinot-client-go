package pinot

import (
	"fmt"
	"net/http"
	"strings"
)

const (
	defaultZkSessionTimeoutSec = 60
)

var grpcTransportFactory = newGrpcBrokerClientTransport

// NewFromBrokerList create a new Pinot connection with pre configured Pinot Broker list.
func NewFromBrokerList(brokerList []string) (*Connection, error) {
	return NewFromBrokerListAndClient(brokerList, http.DefaultClient)
}

// NewFromBrokerListAndClient create a new Pinot connection with pre configured Pinot Broker list and http client.
func NewFromBrokerListAndClient(brokerList []string, httpClient *http.Client) (*Connection, error) {
	clientConfig := &ClientConfig{
		BrokerList: brokerList,
	}
	return NewWithConfigAndClient(clientConfig, httpClient)
}

// NewFromZookeeper create a new Pinot connection through Pinot Zookeeper.
func NewFromZookeeper(zkPath []string, zkPathPrefix string, pinotCluster string) (*Connection, error) {
	return NewFromZookeeperAndClient(zkPath, zkPathPrefix, pinotCluster, http.DefaultClient)
}

// NewFromZookeeperAndClient create a new Pinot connection through Pinot Zookeeper and http client.
func NewFromZookeeperAndClient(zkPath []string, zkPathPrefix string, pinotCluster string, httpClient *http.Client) (*Connection, error) {
	clientConfig := &ClientConfig{
		ZkConfig: &ZookeeperConfig{
			ZookeeperPath:     zkPath,
			PathPrefix:        strings.Join([]string{zkPathPrefix, pinotCluster}, "/"),
			SessionTimeoutSec: defaultZkSessionTimeoutSec,
		},
	}
	return NewWithConfigAndClient(clientConfig, httpClient)
}

// NewFromController creates a new Pinot connection that periodically fetches available brokers via the Controller API.
func NewFromController(controllerAddress string) (*Connection, error) {
	return NewFromControllerAndClient(controllerAddress, http.DefaultClient)
}

// NewFromControllerAndClient creates a new Pinot connection that periodically fetches available brokers via the Controller API.
func NewFromControllerAndClient(controllerAddress string, httpClient *http.Client) (*Connection, error) {
	clientConfig := &ClientConfig{
		ControllerConfig: &ControllerConfig{
			ControllerAddress: controllerAddress,
		},
	}
	return NewWithConfigAndClient(clientConfig, httpClient)
}

// NewWithConfig create a new Pinot connection.
func NewWithConfig(config *ClientConfig) (*Connection, error) {
	return NewWithConfigAndClient(config, http.DefaultClient)
}

// NewWithConfigAndClient create a new Pinot connection with pre-created http client.
func NewWithConfigAndClient(config *ClientConfig, httpClient *http.Client) (*Connection, error) {
	client := httpClient
	if client == nil {
		client = http.DefaultClient
	}
	if config.HTTPTimeout != 0 {
		clientCopy := *client
		clientCopy.Timeout = config.HTTPTimeout
		client = &clientCopy
	}
	var transport clientTransport
	if config.GrpcConfig != nil {
		grpcTransport, err := grpcTransportFactory(config.GrpcConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize grpc transport: %v", err)
		}
		transport = grpcTransport
	} else {
		transport = &jsonAsyncHTTPClientTransport{
			client: client,
			header: config.ExtraHTTPHeader,
		}
	}

	var conn *Connection
	if config.ZkConfig != nil {
		conn = &Connection{
			transport: transport,
			brokerSelector: &dynamicBrokerSelector{
				zkConfig: config.ZkConfig,
			},
			useMultistageEngine: config.UseMultistageEngine,
		}
	}
	if len(config.BrokerList) > 0 {
		conn = &Connection{
			transport: transport,
			brokerSelector: &simpleBrokerSelector{
				brokerList: config.BrokerList,
			},
			useMultistageEngine: config.UseMultistageEngine,
		}
	}
	if config.ControllerConfig != nil {
		conn = &Connection{
			transport: transport,
			brokerSelector: &controllerBasedSelector{
				config: config.ControllerConfig,
				client: client,
			},
			useMultistageEngine: config.UseMultistageEngine,
		}
	}
	if conn != nil {
		// TODO: error handling results into `make test` failure.
		if err := conn.brokerSelector.init(); err != nil {
			return conn, fmt.Errorf("failed to initialize broker selector: %v", err)
		}
		return conn, nil
	}
	return nil, fmt.Errorf(
		"please specify at least one of Pinot Zookeeper, Pinot Broker or Pinot Controller to connect",
	)
}
