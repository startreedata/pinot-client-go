package pinot

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPinotClients(t *testing.T) {
	pinotClient1, err := NewFromZookeeper([]string{"localhost:2181"}, "", "QuickStartCluster")
	assert.NotNil(t, pinotClient1)
	assert.NotNil(t, pinotClient1.brokerSelector)
	assert.NotNil(t, pinotClient1.transport)
	assert.Nil(t, err)
	pinotClient2, err := NewWithConfig(&ClientConfig{
		ZkConfig: &ZookeeperConfig{
			ZookeeperPath:     []string{"localhost:2181"},
			PathPrefix:        strings.Join([]string{"/", "QuickStartCluster"}, "/"),
			SessionTimeoutSec: defaultZkSessionTimeoutSec,
		},
		ExtraHTTPHeader: map[string]string{
			"k1": "v1",
		},
	})
	assert.NotNil(t, pinotClient2)
	assert.NotNil(t, pinotClient2.brokerSelector)
	assert.NotNil(t, pinotClient2.transport)
	assert.Nil(t, err)
	pinotClient3, err := NewFromController("localhost:9000")
	assert.NotNil(t, pinotClient3)
	assert.NotNil(t, pinotClient3.brokerSelector)
	assert.NotNil(t, pinotClient3.transport)
	_, err = NewWithConfig(&ClientConfig{})
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "please specify"))
}
