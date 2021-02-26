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

// NewWithConfig create a new Pinot connection.
func NewWithConfig(config *ClientConfig) (*Connection, error) {
	tansport := &jsonAsyncHTTPClientTransport{
		client: http.DefaultClient,
		header: config.ExtraHTTPHeader,
	}
	var conn *Connection
	if config.ZkConfig != nil {
		conn = &Connection{
			transport: tansport,
			brokerSelector: &dynamicBrokerSelector{
				zkConfig: config.ZkConfig,
			},
		}
	}
	if config.BrokerList != nil && len(config.BrokerList) > 0 {
		conn = &Connection{
			transport: tansport,
			brokerSelector: &simpleBrokerSelector{
				brokerList: config.BrokerList,
			},
		}
	}
	if conn != nil {
		conn.brokerSelector.init()
		return conn, nil
	}
	return nil, fmt.Errorf("please specify at least one of Pinot Zookeeper or Pinot Broker to connect")
}
