package pinot

import (
	"strconv"
	"strings"
)

type brokerDto struct {
	Host         string `json:"host"`
	InstanceName string `json:"instanceName"`
	Port         int    `json:"port"`
}

type controllerResponse map[string]([]brokerDto)

func (b *brokerDto) extractBrokerName() string {
	return strings.Join([]string{b.Host, strconv.Itoa(b.Port)}, ":")
}

func (r *controllerResponse) extractBrokerList() []string {
	brokerSet := map[string]struct{}{}
	for _, brokers := range *r {
		for _, broker := range brokers {
			brokerSet[broker.extractBrokerName()] = struct{}{}
		}
	}
	brokerList := make([]string, 0, len(brokerSet))

	for key := range brokerSet {
		brokerList = append(brokerList, key)
	}
	return brokerList
}

func (r *controllerResponse) extractTableToBrokerMap() map[string]([]string) {
	tableToBrokerMap := make(map[string]([]string))
	for table, brokers := range *r {
		brokersPerTable := make([]string, 0, len(brokers))
		for _, broker := range brokers {
			brokersPerTable = append(brokersPerTable, broker.extractBrokerName())
		}
		tableToBrokerMap[table] = brokersPerTable
	}
	return tableToBrokerMap
}
