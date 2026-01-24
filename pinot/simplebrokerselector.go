package pinot

import (
	"fmt"
	"math/rand"
)

type simpleBrokerSelector struct {
	brokerList []string
}

func (s *simpleBrokerSelector) init() error {
	if len(s.brokerList) == 0 {
		return fmt.Errorf("no pre-configured broker lists set in simpleBrokerSelector")
	}
	return nil
}

func (s *simpleBrokerSelector) selectBroker(_ string) (string, error) {
	if len(s.brokerList) == 0 {
		return "", fmt.Errorf("no pre-configured broker lists set in simpleBrokerSelector")
	}
	// #nosec G404
	return s.brokerList[rand.Intn(len(s.brokerList))], nil
}
