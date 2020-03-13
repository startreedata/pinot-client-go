package pinot

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractTableName(t *testing.T) {
	assert.Equal(t, "table", extractTableName("table_OFFLINE"))
	assert.Equal(t, "table", extractTableName("table_REALTIME"))
	assert.Equal(t, "table", extractTableName("table"))
}

func TestExtractBrokers(t *testing.T) {
	brokers := extractBrokers(map[string]string{
		"BROKER_broker-1_1000": "ONLINE",
		"BROKER_broker-2_1000": "ONLINE",
	})
	assert.Equal(t, 2, len(brokers))
	assert.True(t, brokers[0] == "broker-1:1000" || brokers[0] == "broker-2:1000")
	assert.True(t, brokers[1] == "broker-1:1000" || brokers[1] == "broker-2:1000")
}

func TestExtractBrokerHostPort(t *testing.T) {
	host, port, err := extractBrokerHostPort("BROKER_broker-1_1000")
	assert.Equal(t, "broker-1", host)
	assert.Equal(t, "1000", port)
	assert.Nil(t, err)
	_, _, err = extractBrokerHostPort("broker-1:1000")
	assert.NotNil(t, err)
	_, _, err = extractBrokerHostPort("BROKER_broker-1_aaa")
	assert.NotNil(t, err)
}
