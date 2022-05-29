package pinot

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
)

const (
	offlineSuffix  = "_OFFLINE"
	realtimeSuffix = "_REALTIME"
)

type tableAwareBrokerSelector struct {
	tableBrokerMap map[string]([]string)
	allBrokerList  []string
	rwMux          sync.RWMutex
}

func (s *tableAwareBrokerSelector) selectBroker(table string) (string, error) {
	tableName := extractTableName(table)
	var brokerList []string
	if tableName == "" {
		s.rwMux.RLock()
		brokerList = s.allBrokerList
		s.rwMux.RUnlock()
		if len(brokerList) == 0 {
			return "", fmt.Errorf("No available broker found")
		}
	} else {
		var found bool
		s.rwMux.RLock()
		brokerList, found = s.tableBrokerMap[tableName]
		s.rwMux.RUnlock()
		if !found {
			return "", fmt.Errorf("Unable to find the table: %s", table)
		}
		if len(brokerList) == 0 {
			return "", fmt.Errorf("No available broker found for table: %s", table)
		}
	}
	return brokerList[rand.Intn(len(brokerList))], nil
}

func extractTableName(table string) string {
	return strings.Replace(strings.Replace(table, offlineSuffix, "", 1), realtimeSuffix, "", 1)
}
