package main

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	pinot "github.com/startreedata/pinot-client-go/pinot"
)

func main() {
	pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
		BrokerList: []string{"localhost:8010"},
		GrpcConfig: &pinot.GrpcConfig{
			Encoding:     "JSON",
			Compression:  "ZSTD",
			BlockRowSize: 10000,
			Timeout:      5 * time.Second,
			// TLSConfig: &pinot.GrpcTLSConfig{
			// 	Enabled:    true,
			// 	CACertPath: "/path/to/ca.pem",
			// },
		},
	})
	if err != nil {
		log.Fatalln(err)
	}

	table := "baseballStats"
	query := "select * from baseballStats where teamID = 'OAK' and yearID = 2004 order by homeRuns desc limit 5"
	brokerResp, err := pinotClient.ExecuteSQL(table, query)
	if err != nil {
		log.Fatalln(err)
	}
	printBrokerResp(brokerResp)
}

func printBrokerResp(brokerResp *pinot.BrokerResponse) {
	log.Infof("Query Stats: response time - %d ms, scanned docs - %d, total docs - %d", brokerResp.TimeUsedMs, brokerResp.NumDocsScanned, brokerResp.TotalDocs)
	if len(brokerResp.Exceptions) > 0 {
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
	}
}
