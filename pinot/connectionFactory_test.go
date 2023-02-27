package pinot

import (
	"strings"
	"testing"
	"time"

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

func TestPinotWithHttpTImeout(t *testing.T) {
	pinotClient, err := NewWithConfig(&ClientConfig{
		// Hit an unreachable port
		BrokerList: []string{"www.google.com:81"},
		// Set timeout to 1 sec
		HTTPTimeout: 1 * time.Second,
	})
	assert.Nil(t, err)
	start := time.Now()
	_, err = pinotClient.ExecuteSQL("testTable", "select * from testTable")
	end := time.Since(start)
	assert.NotNil(t, err)
	diff := int(end.Seconds())
	// Query should ideally timeout in 1 sec, considering other variables,
	// diff might not be exactly equal to 1. So, we can assert that diff
	// must be less than 2 sec.
	assert.Less(t, diff, 2)
}
