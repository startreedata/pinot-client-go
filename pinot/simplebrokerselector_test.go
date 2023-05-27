package pinot

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleBrokerSelector(t *testing.T) {
	s := &simpleBrokerSelector{
		brokerList: []string{
			"broker0",
			"broker1",
			"broker2",
			"broker3",
			"broker4",
		},
	}
	err := s.init()
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		brokerName, err := s.selectBroker("")
		assert.Equal(t, "broker", brokerName[0:6])
		assert.Nil(t, err)
		brokerName, err = s.selectBroker("t")
		assert.Equal(t, "broker", brokerName[0:6])
		assert.Nil(t, err)
	}
}

func TestWithEmptyBrokerList(t *testing.T) {
	s := &simpleBrokerSelector{
		brokerList: []string{},
	}
	err := s.init()
	assert.EqualError(t, err, "No pre-configured broker lists set in simpleBrokerSelector")
	for i := 0; i < 10; i++ {
		brokerName, err := s.selectBroker("t")
		assert.Equal(t, "", brokerName)
		assert.EqualError(t, err, "No pre-configured broker lists set in simpleBrokerSelector")
	}
}
