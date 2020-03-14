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

func TestErrorInit(t *testing.T) {
	selector := &dynamicBrokerSelector{
		zkConfig: &ZookeeperConfig{
			ZookeeperPath: []string{},
		},
		tableBrokerMap: map[string]([]string){"myTable": []string{}},
	}
	err := selector.init()
	assert.NotNil(t, err)
}

func TestSelectBroker(t *testing.T) {
	selector := &dynamicBrokerSelector{
		tableBrokerMap: map[string]([]string){"myTable": []string{"localhost:8000"}},
		allBrokerList:  []string{"localhost:8000"},
	}
	broker, err := selector.selectBroker("")
	assert.Equal(t, "localhost:8000", broker)
	assert.Nil(t, err)
	broker, err = selector.selectBroker("myTable")
	assert.Equal(t, "localhost:8000", broker)
	assert.Nil(t, err)
	_, err = selector.selectBroker("unexistTable")
	assert.NotNil(t, err)
}

func TestErrorSelectBroker(t *testing.T) {
	emptySelector := &dynamicBrokerSelector{
		tableBrokerMap: map[string]([]string){"myTable": []string{}},
	}
	_, err := emptySelector.selectBroker("")
	assert.NotNil(t, err)
	_, err = emptySelector.selectBroker("myTable")
	assert.NotNil(t, err)
	_, err = emptySelector.selectBroker("unexistTable")
	assert.NotNil(t, err)
}

func TestExternalViewUpdate(t *testing.T) {
	evBytes := []byte(`{"id":"brokerResource","simpleFields":{"BATCH_MESSAGE_MODE":"false","BUCKET_SIZE":"0","IDEAL_STATE_MODE":"CUSTOMIZED","NUM_PARTITIONS":"1","REBALANCE_MODE":"CUSTOMIZED","REPLICAS":"0","STATE_MODEL_DEF_REF":"BrokerResourceOnlineOfflineStateModel","STATE_MODEL_FACTORY_NAME":"DEFAULT"},"mapFields":{"baseballStats_OFFLINE":{"Broker_127.0.0.1_8000":"ONLINE", "Broker_127.0.0.1_9000":"ONLINE"}},"listFields":{}}`)
	ev, err := getExternalView(evBytes)
	assert.NotNil(t, ev)
	assert.Nil(t, err)
	assert.Equal(t, "brokerResource", ev.ID)
	assert.Equal(t, "false", ev.SimpleFields["BATCH_MESSAGE_MODE"])
	assert.Equal(t, 2, len(ev.MapFields["baseballStats_OFFLINE"]))
	assert.Equal(t, "ONLINE", ev.MapFields["baseballStats_OFFLINE"]["Broker_127.0.0.1_8000"])

	tableBrokerMap, allBrokerList := generateNewBrokerMappingExternalView(ev)
	assert.Equal(t, 1, len(tableBrokerMap))
	assert.Equal(t, 2, len(tableBrokerMap["baseballStats"]))
	for i := 0; i < 2; i++ {
		assert.True(t, tableBrokerMap["baseballStats"][i] == "127.0.0.1:8000" || tableBrokerMap["baseballStats"][i] == "127.0.0.1:9000")
	}
	assert.Equal(t, 2, len(allBrokerList))
	for i := 0; i < 2; i++ {
		assert.True(t, allBrokerList[i] == "127.0.0.1:8000" || allBrokerList[i] == "127.0.0.1:9000")
	}
}

func TestErrorExternalViewUpdate(t *testing.T) {
	ev, err := getExternalView([]byte(`random`))
	assert.Nil(t, ev)
	assert.NotNil(t, err)
}
