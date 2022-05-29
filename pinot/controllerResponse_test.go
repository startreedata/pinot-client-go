package pinot

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractBrokerName(t *testing.T) {
	b := &brokerDto{
		Port:         8000,
		Host:         "testHost",
		InstanceName: "Broker_testHost_8000",
	}
	name := b.extractBrokerName()
	assert.Equal(t, "testHost:8000", name)
}

func TestExtractBrokerList(t *testing.T) {
	r := &controllerResponse{
		"table1": {
			{

				Port:         8000,
				Host:         "testHost1",
				InstanceName: "Broker_testHost1_8000",
			},
			{

				Port:         8000,
				Host:         "testHost2",
				InstanceName: "Broker_testHost2_8000",
			},
		},
		"table2": {
			{

				Port:         8000,
				Host:         "testHost2",
				InstanceName: "Broker_testHost2_8000",
			},
			{

				Port:         8123,
				Host:         "testHost3",
				InstanceName: "Broker_testHost3_8123",
			},
		},
	}
	brokerList := r.extractBrokerList()
	assert.ElementsMatch(
		t,
		[]string{"testHost1:8000", "testHost2:8000", "testHost3:8123"},
		brokerList,
	)
}

func TestExtractBrokerListEmpty(t *testing.T) {
	r := &controllerResponse{}
	brokerList := r.extractBrokerList()
	assert.Len(t, brokerList, 0)
}

func TestExtractTableToBrokerMap(t *testing.T) {
	r := &controllerResponse{
		"table1": {
			{

				Port:         8000,
				Host:         "testHost1",
				InstanceName: "Broker_testHost1_8000",
			},
			{

				Port:         8000,
				Host:         "testHost2",
				InstanceName: "Broker_testHost2_8000",
			},
		},
		"table2": {
			{

				Port:         8000,
				Host:         "testHost2",
				InstanceName: "Broker_testHost2_8000",
			},
			{

				Port:         8123,
				Host:         "testHost3",
				InstanceName: "Broker_testHost3_8123",
			},
		},
	}
	tableToBrokerMap := r.extractTableToBrokerMap()
	expected := map[string]([]string){
		"table1": {
			"testHost1:8000",
			"testHost2:8000",
		},
		"table2": {
			"testHost2:8000",
			"testHost3:8123",
		},
	}
	assert.True(t, reflect.DeepEqual(tableToBrokerMap, expected))
}
