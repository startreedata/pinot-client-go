package pinot

import (
	"fmt"
	"net/http"
	"strings"
)

const (
	defaultZkSessionTimeoutSec = 60
)

// NewFromBrokerList create a new Pinot connection with pre configured Pinot Broker list.
func NewFromBrokerList(brokerList []string) (*Connection, error) {
	clientConfig := &ClientConfig{
		BrokerList: brokerList,
	}
	return NewWithConfig(clientConfig)
}

// NewFromZookeeper create a new Pinot connection through Pinot Zookeeper.
func NewFromZookeeper(zkPath []string, zkPathPrefix string, pinotCluster string) (*Connection, error) {
	clientConfig := &ClientConfig{
		ZkConfig: &ZookeeperConfig{
			ZookeeperPath:     zkPath,
			PathPrefix:        strings.Join([]string{zkPathPrefix, pinotCluster}, "/"),
			SessionTimeoutSec: defaultZkSessionTimeoutSec,
		},
	}
	return NewWithConfig(clientConfig)
}

// NewFromController creates a new Pinot connection that periodically fetches available brokers via the Controller API.
func NewFromController(controllerAddress string) (*Connection, error) {
	clientConfig := &ClientConfig{
		ControllerConfig: &ControllerConfig{
			ControllerAddress: controllerAddress,
		},
	}
	return NewWithConfig(clientConfig)
}

// NewWithConfig create a new Pinot connection.
func NewWithConfig(config *ClientConfig) (*Connection, error) {
	transport := &jsonAsyncHTTPClientTransport{
		client: http.DefaultClient,
		header: config.ExtraHTTPHeader,
	}

	// Set HTTPTimeout from config
	if config.HTTPTimeout != 0 {
		transport.client.Timeout = config.HTTPTimeout
	}

	var conn *Connection
	if config.ZkConfig != nil {
		conn = &Connection{
			transport: transport,
			brokerSelector: &dynamicBrokerSelector{
				zkConfig: config.ZkConfig,
			},
		}
	}
	if config.BrokerList != nil && len(config.BrokerList) > 0 {
		conn = &Connection{
			transport: transport,
			brokerSelector: &simpleBrokerSelector{
				brokerList: config.BrokerList,
			},
		}
	}
	if config.ControllerConfig != nil {
		conn = &Connection{
			transport: transport,
			brokerSelector: &controllerBasedSelector{
				config: config.ControllerConfig,
				client: http.DefaultClient,
			},
		}
	}
	if conn != nil {
		// TODO: error handling results into `make test` failure.
		conn.brokerSelector.init()
		return conn, nil
	}
	return nil, fmt.Errorf(
		"please specify at least one of Pinot Zookeeper, Pinot Broker or Pinot Controller to connect",
	)
}
