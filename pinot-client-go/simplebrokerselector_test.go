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
	for i := 0; i < 10; i++ {
		brokerName, err := s.selectBroker("t")
		assert.Equal(t, "broker", brokerName[0:6])
		assert.Nil(t, err)
	}

}
