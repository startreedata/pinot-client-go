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

func TestSelectBroker(t *testing.T) {
	selector := &tableAwareBrokerSelector{
		tableBrokerMap: map[string][]string{"myTable": []string{"localhost:8000"}},
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
	emptySelector := &tableAwareBrokerSelector{
		tableBrokerMap: map[string][]string{"myTable": []string{}},
	}
	_, err := emptySelector.selectBroker("")
	assert.NotNil(t, err)
	_, err = emptySelector.selectBroker("myTable")
	assert.NotNil(t, err)
	_, err = emptySelector.selectBroker("unexistTable")
	assert.NotNil(t, err)
}
