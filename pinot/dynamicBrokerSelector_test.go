package pinot

import (
	"fmt"
	"testing"
	"time"

	zk "github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

type fakeZkClient struct {
	getBytes []byte
	getErr   error
	getWErr  error
	watch    <-chan zk.Event
}

func (f *fakeZkClient) Get(_ string) ([]byte, *zk.Stat, error) {
	return f.getBytes, &zk.Stat{}, f.getErr
}

func (f *fakeZkClient) GetW(_ string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	return nil, &zk.Stat{}, f.watch, f.getWErr
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
		tableAwareBrokerSelector: tableAwareBrokerSelector{
			tableBrokerMap: map[string][]string{"myTable": {}},
		},
		zkConfig: &ZookeeperConfig{
			ZookeeperPath: []string{},
		},
	}
	err := selector.init()
	assert.NotNil(t, err)
}

func TestInitSuccess(t *testing.T) {
	originalConnect := zkConnect
	watch := make(chan zk.Event)
	defer func() {
		zkConnect = originalConnect
		close(watch)
	}()

	zkConnect = func(_ []string, _ time.Duration) (zkClient, <-chan zk.Event, error) {
		return &fakeZkClient{
			getBytes: []byte(`{"id":"brokerResource","mapFields":{"baseballStats_OFFLINE":{"Broker_127.0.0.1_8000":"ONLINE"}}}`),
			watch:    watch,
		}, watch, nil
	}

	selector := &dynamicBrokerSelector{
		zkConfig: &ZookeeperConfig{
			ZookeeperPath:     []string{"localhost:2123"},
			PathPrefix:        "/QuickStartCluster",
			SessionTimeoutSec: 1,
		},
	}
	err := selector.init()
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:8000", selector.allBrokerList[0])
	assert.Len(t, selector.tableBrokerMap["baseballStats"], 1)
}

func TestInitGetWError(t *testing.T) {
	originalConnect := zkConnect
	defer func() { zkConnect = originalConnect }()

	zkConnect = func(_ []string, _ time.Duration) (zkClient, <-chan zk.Event, error) {
		return &fakeZkClient{
			getBytes: []byte(`{"id":"brokerResource","mapFields":{}}`),
			getWErr:  fmt.Errorf("getw failed"),
		}, nil, nil
	}

	selector := &dynamicBrokerSelector{
		zkConfig: &ZookeeperConfig{
			ZookeeperPath:     []string{"localhost:2123"},
			PathPrefix:        "/QuickStartCluster",
			SessionTimeoutSec: 1,
		},
	}
	err := selector.init()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to set a watcher")
}

func TestSetupWatcherHandlesEvents(_ *testing.T) {
	selector := &dynamicBrokerSelector{
		externalViewZkPath: "path",
	}
	selector.readZNode = func(_ string) ([]byte, error) {
		return []byte(`{"id":"brokerResource","mapFields":{"baseballStats_OFFLINE":{"Broker_127.0.0.1_8000":"ONLINE"}}}`), nil
	}

	ch := make(chan zk.Event, 2)
	ch <- zk.Event{Err: fmt.Errorf("watch error")}
	ch <- zk.Event{Type: zk.EventNodeDataChanged}
	selector.externalViewZnodeWatch = ch

	go selector.setupWatcher()

	close(ch)
	time.Sleep(50 * time.Millisecond)
}

func TestInitRefreshExternalViewError(t *testing.T) {
	originalConnect := zkConnect
	defer func() { zkConnect = originalConnect }()

	zkConnect = func(_ []string, _ time.Duration) (zkClient, <-chan zk.Event, error) {
		return &fakeZkClient{
			getErr: fmt.Errorf("read error"),
			watch:  make(chan zk.Event),
		}, make(chan zk.Event), nil
	}

	selector := &dynamicBrokerSelector{
		zkConfig: &ZookeeperConfig{
			ZookeeperPath:     []string{"localhost:2123"},
			PathPrefix:        "/QuickStartCluster",
			SessionTimeoutSec: 1,
		},
	}
	err := selector.init()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to read zk")
}

func TestSetupWatcherRefreshError(_ *testing.T) {
	selector := &dynamicBrokerSelector{
		externalViewZkPath: "path",
	}
	selector.readZNode = func(_ string) ([]byte, error) {
		return nil, fmt.Errorf("read error")
	}

	ch := make(chan zk.Event, 1)
	ch <- zk.Event{Type: zk.EventNodeDataChanged}
	selector.externalViewZnodeWatch = ch

	go selector.setupWatcher()

	close(ch)
	time.Sleep(50 * time.Millisecond)
}

func TestErrorRefreshExternalView(t *testing.T) {
	selector := &dynamicBrokerSelector{
		tableAwareBrokerSelector: tableAwareBrokerSelector{
			tableBrokerMap: map[string][]string{"myTable": {}},
		},
		zkConfig: &ZookeeperConfig{
			ZookeeperPath: []string{},
		},
	}
	err := selector.refreshExternalView()
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

func TestMockReadZNode(t *testing.T) {
	evBytes := []byte(`{"id":"brokerResource","simpleFields":{"BATCH_MESSAGE_MODE":"false","BUCKET_SIZE":"0","IDEAL_STATE_MODE":"CUSTOMIZED","NUM_PARTITIONS":"1","REBALANCE_MODE":"CUSTOMIZED","REPLICAS":"0","STATE_MODEL_DEF_REF":"BrokerResourceOnlineOfflineStateModel","STATE_MODEL_FACTORY_NAME":"DEFAULT"},"mapFields":{"baseballStats_OFFLINE":{"Broker_127.0.0.1_8000":"ONLINE", "Broker_127.0.0.1_9000":"ONLINE"}},"listFields":{}}`)
	selector := &dynamicBrokerSelector{
		readZNode: func(_ string) ([]byte, error) {
			return evBytes, nil
		},
	}
	err := selector.refreshExternalView()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(selector.tableBrokerMap))
	assert.Equal(t, 2, len(selector.tableBrokerMap["baseballStats"]))
	for i := 0; i < 2; i++ {
		assert.True(t, selector.tableBrokerMap["baseballStats"][i] == "127.0.0.1:8000" || selector.tableBrokerMap["baseballStats"][i] == "127.0.0.1:9000")
	}
	assert.Equal(t, 2, len(selector.allBrokerList))
	for i := 0; i < 2; i++ {
		assert.True(t, selector.allBrokerList[i] == "127.0.0.1:8000" || selector.allBrokerList[i] == "127.0.0.1:9000")
	}

	evBytes = []byte(`{"id":"brokerResource","simpleFields":{"BATCH_MESSAGE_MODE":"false","BUCKET_SIZE":"0","IDEAL_STATE_MODE":"CUSTOMIZED","NUM_PARTITIONS":"1","REBALANCE_MODE":"CUSTOMIZED","REPLICAS":"0","STATE_MODEL_DEF_REF":"BrokerResourceOnlineOfflineStateModel","STATE_MODEL_FACTORY_NAME":"DEFAULT"},"mapFields":{"baseballStats_OFFLINE":{"Broker_127.0.0.1_8000":"ONLINE"}},"listFields":{}}`)
	err = selector.refreshExternalView()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(selector.tableBrokerMap))
	assert.Equal(t, 1, len(selector.tableBrokerMap["baseballStats"]))
	assert.True(t, selector.tableBrokerMap["baseballStats"][0] == "127.0.0.1:8000")
	assert.Equal(t, 1, len(selector.allBrokerList))
	assert.True(t, selector.allBrokerList[0] == "127.0.0.1:8000")

	evBytes = []byte(`abc`)
	err = selector.refreshExternalView()
	assert.NotNil(t, err)
	selector.readZNode = func(_ string) ([]byte, error) {
		return nil, fmt.Errorf("erroReadZNode")
	}
	err = selector.refreshExternalView()
	assert.EqualError(t, err, "erroReadZNode")
}

func TestMockUpdateEvent(t *testing.T) {
	evBytes := []byte(`{"id":"brokerResource","simpleFields":{"BATCH_MESSAGE_MODE":"false","BUCKET_SIZE":"0","IDEAL_STATE_MODE":"CUSTOMIZED","NUM_PARTITIONS":"1","REBALANCE_MODE":"CUSTOMIZED","REPLICAS":"0","STATE_MODEL_DEF_REF":"BrokerResourceOnlineOfflineStateModel","STATE_MODEL_FACTORY_NAME":"DEFAULT"},"mapFields":{"baseballStats_OFFLINE":{"Broker_127.0.0.1_8000":"ONLINE", "Broker_127.0.0.1_9000":"ONLINE"}},"listFields":{}}`)
	ch := make(chan zk.Event)
	selector := &dynamicBrokerSelector{
		readZNode: func(_ string) ([]byte, error) {
			return evBytes, nil
		},
		externalViewZnodeWatch: ch,
	}
	go selector.setupWatcher()
	err := selector.refreshExternalView()
	assert.Nil(t, err)
	selector.rwMux.RLock()
	assert.Equal(t, 1, len(selector.tableBrokerMap))
	assert.Equal(t, 2, len(selector.tableBrokerMap["baseballStats"]))
	for i := 0; i < 2; i++ {
		assert.True(t, selector.tableBrokerMap["baseballStats"][i] == "127.0.0.1:8000" || selector.tableBrokerMap["baseballStats"][i] == "127.0.0.1:9000")
	}
	assert.Equal(t, 2, len(selector.allBrokerList))
	for i := 0; i < 2; i++ {
		assert.True(t, selector.allBrokerList[i] == "127.0.0.1:8000" || selector.allBrokerList[i] == "127.0.0.1:9000")
	}
	selector.rwMux.RUnlock()
	// Give another event
	evBytes = []byte(`{"id":"brokerResource","simpleFields":{"BATCH_MESSAGE_MODE":"false","BUCKET_SIZE":"0","IDEAL_STATE_MODE":"CUSTOMIZED","NUM_PARTITIONS":"1","REBALANCE_MODE":"CUSTOMIZED","REPLICAS":"0","STATE_MODEL_DEF_REF":"BrokerResourceOnlineOfflineStateModel","STATE_MODEL_FACTORY_NAME":"DEFAULT"},"mapFields":{"baseballStats_OFFLINE":{"Broker_127.0.0.1_8000":"ONLINE"}},"listFields":{}}`)
	ch <- zk.Event{
		Type: zk.EventNodeDataChanged,
	}
	time.Sleep(300 * time.Millisecond)
	selector.rwMux.RLock()
	assert.Equal(t, 1, len(selector.tableBrokerMap))
	assert.Equal(t, 1, len(selector.tableBrokerMap["baseballStats"]))
	assert.True(t, selector.tableBrokerMap["baseballStats"][0] == "127.0.0.1:8000")
	assert.Equal(t, 1, len(selector.allBrokerList))
	assert.True(t, selector.allBrokerList[0] == "127.0.0.1:8000")
	selector.rwMux.RUnlock()

	evBytes = []byte(`abc`)
	err = selector.refreshExternalView()
	assert.NotNil(t, err)
	selector.readZNode = func(_ string) ([]byte, error) {
		return nil, fmt.Errorf("erroReadZNode")
	}
	err = selector.refreshExternalView()
	assert.EqualError(t, err, "erroReadZNode")
}
