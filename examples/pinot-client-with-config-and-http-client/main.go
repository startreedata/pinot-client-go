package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	pinot "github.com/startreedata/pinot-client-go/pinot"

	log "github.com/sirupsen/logrus"
)

func connectPinot() *pinot.Connection {
	httpClient := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100, // Max idle connections in total
			MaxIdleConnsPerHost: 10,  // Max idle connections per host
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			// You may add other settings like TLS configuration, Proxy, etc.
		},
	}
	pinotClient, err := pinot.NewWithConfigAndClient(&pinot.ClientConfig{
		BrokerList:  []string{"https://broker.pinot.myorg.mycompany.startree.cloud"},
		HTTPTimeout: 1500 * time.Millisecond,
		ExtraHTTPHeader: map[string]string{
			"authorization": "Basic <API-TOKEN>",
		},
	}, httpClient)

	if err != nil {
		log.Fatalln(err)
	}

	if pinotClient != nil {
		log.Infof("Successfully established connection with Pinot Server!")
	}
	return pinotClient
}

func main() {
	pinotClient := connectPinot()

	table := "airlineStats"

	pinotQueries := []string{
		"select count(*) as cnt from airlineStats limit 1",
	}

	log.Printf("Querying SQL")
	for _, query := range pinotQueries {
		log.Printf("Trying to query Pinot: %v\n", query)
		brokerResp, err := pinotClient.ExecuteSQL(table, query)
		if err != nil {
			log.Fatalln(err)
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
