package main

import (
	"encoding/json"
	"fmt"

	pinot "github.com/startreedata/pinot-client-go/pinot"

	log "github.com/sirupsen/logrus"
)

func main() {
	pinotClient, err := pinot.NewFromZookeeper([]string{"localhost:2123"}, "", "QuickStartCluster")
	if err != nil {
		log.Error(err)
	}
	table := "baseballStats"
	pinotQueries := []string{
		"select * from baseballStats limit 10",
		"select count(*) as cnt from baseballStats limit 1",
		"select count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats limit 1",
		"select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10",
		"select max(league) from baseballStats limit 10",
	}

	log.Infof("Querying SQL")
	for _, query := range pinotQueries {
		log.Infof("Trying to query Pinot: %v", query)
		brokerResp, err := pinotClient.ExecuteSQL(table, query)
		if err != nil {
			log.Error(err)
		}
		printBrokerResp(brokerResp)
	}
}

func printBrokerResp(brokerResp *pinot.BrokerResponse) {
	log.Infof("Query Stats: response time - %d ms, scanned docs - %d, total docs - %d", brokerResp.TimeUsedMs, brokerResp.NumDocsScanned, brokerResp.TotalDocs)
	if brokerResp.Exceptions != nil && len(brokerResp.Exceptions) > 0 {
		jsonBytes, _ := json.Marshal(brokerResp.Exceptions)
		log.Infof("brokerResp.Exceptions:\n%s\n", jsonBytes)
		return
	}
	if brokerResp.ResultTable != nil {
		jsonBytes, _ := json.Marshal(brokerResp.ResultTable)
		log.Infof("brokerResp.ResultTable:\n%s\n", jsonBytes)
		line := ""
		for c := 0; c < brokerResp.ResultTable.GetColumnCount(); c++ {
			line += fmt.Sprintf("%s(%s)\t", brokerResp.ResultTable.GetColumnName(c), brokerResp.ResultTable.GetColumnDataType(c))
		}
		line += "\n"
		for r := 0; r < brokerResp.ResultTable.GetRowCount(); r++ {
			for c := 0; c < brokerResp.ResultTable.GetColumnCount(); c++ {
				line += fmt.Sprintf("%v\t", brokerResp.ResultTable.Get(r, c))
			}
			line += "\n"
		}
		log.Infof("ResultTable:\n%s", line)
		return
	}
	if brokerResp.AggregationResults != nil {
		jsonBytes, _ := json.Marshal(brokerResp.AggregationResults)
		log.Infof("brokerResp.AggregationResults:\n%s\n", jsonBytes)
		return
	}
	if brokerResp.SelectionResults != nil {
		jsonBytes, _ := json.Marshal(brokerResp.SelectionResults)
		log.Infof("brokerResp.SelectionResults:\n%s\n", jsonBytes)
		return
	}
}
