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
		return fmt.Errorf("No pre-configured broker lists set in simpleBrokerSelector")
	}
	return nil
}

func (s *simpleBrokerSelector) selectBroker(table string) (string, error) {
	if len(s.brokerList) == 0 {
		return "", fmt.Errorf("No pre-configured broker lists set in simpleBrokerSelector")
	}
	return s.brokerList[rand.Intn(len(s.brokerList))], nil
}
