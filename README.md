Pinot Client GO
===============
[![GoDoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/startreedata/pinot-client-go)
[![Build Status](https://travis-ci.com/startreedata/pinot-client-go.svg?branch=master)](https://travis-ci.com/startreedata/pinot-client-go)
[![Coverage Status](https://coveralls.io/repos/github/startreedata/pinot-client-go/badge.svg?branch=master)](https://coveralls.io/github/startreedata/pinot-client-go?branch=master)

![image](https://user-images.githubusercontent.com/1202120/116982228-63315900-ac7d-11eb-96e5-01a04ef7d737.png)

Applications can use this golang client library to query Apache Pinot.

Examples
========

Please follow this [Pinot Quickstart](https://docs.pinot.apache.org/basics/getting-started/running-pinot-locally) link to install and start Pinot batch quickstart locally.

```
bin/quick-start-batch.sh
```

Check out Client library Github Repo

```
git clone git@github.com:startreedata/pinot-client-go.git
cd pinot-client-go
```

Build and run the example application to query from Pinot Batch Quickstart

```
go build ./examples/batch-quickstart
./batch-quickstart
```

Usage
=====

Create a Pinot Connection
-------------------------

Pinot client could be initialized through:

1. Zookeeper Path.

```
pinotClient := pinot.NewFromZookeeper([]string{"localhost:2123"}, "", "QuickStartCluster")
```

2. A list of broker addresses.

```
pinotClient := pinot.NewFromBrokerList([]string{"localhost:8000"})
```

3. ClientConfig

```
pinotClient := pinot.NewWithConfig(&pinot.ClientConfig{
	ZkConfig: &pinot.ZookeeperConfig{
		ZookeeperPath:     zkPath,
		PathPrefix:        strings.Join([]string{zkPathPrefix, pinotCluster}, "/"),
		SessionTimeoutSec: defaultZkSessionTimeoutSec,
	},
    ExtraHTTPHeader: map[string]string{
        "extra-header":"value",
    },
})
```

Query Pinot
-----------

Please see this [example](https://github.com/startreedata/pinot-client-go/blob/master/examples/batch-quickstart/main.go) for your reference.

Code snippet:
```
pinotClient, err := pinot.NewFromZookeeper([]string{"localhost:2123"}, "", "QuickStartCluster")
if err != nil {
    log.Error(err)
}
brokerResp, err := pinotClient.ExecuteSQL("baseballStats", "select count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
if err != nil {
    log.Error(err)
}
log.Infof("Query Stats: response time - %d ms, scanned docs - %d, total docs - %d", brokerResp.TimeUsedMs, brokerResp.NumDocsScanned, brokerResp.TotalDocs)
```

Response Format
---------------

Query Response is defined as the struct of following:

```
type BrokerResponse struct {
	AggregationResults          []*AggregationResult `json:"aggregationResults,omitempty"`
	SelectionResults            *SelectionResults    `json:"SelectionResults,omitempty"`
	ResultTable                 *ResultTable         `json:"resultTable,omitempty"`
	Exceptions                  []Exception          `json:"exceptions"`
	TraceInfo                   map[string]string    `json:"traceInfo,omitempty"`
	NumServersQueried           int                  `json:"numServersQueried"`
	NumServersResponded         int                  `json:"numServersResponded"`
	NumSegmentsQueried          int                  `json:"numSegmentsQueried"`
	NumSegmentsProcessed        int                  `json:"numSegmentsProcessed"`
	NumSegmentsMatched          int                  `json:"numSegmentsMatched"`
	NumConsumingSegmentsQueried int                  `json:"numConsumingSegmentsQueried"`
	NumDocsScanned              int64                `json:"numDocsScanned"`
	NumEntriesScannedInFilter   int64                `json:"numEntriesScannedInFilter"`
	NumEntriesScannedPostFilter int64                `json:"numEntriesScannedPostFilter"`
	NumGroupsLimitReached       bool                 `json:"numGroupsLimitReached"`
	TotalDocs                   int64                `json:"totalDocs"`
	TimeUsedMs                  int                  `json:"timeUsedMs"`
	MinConsumingFreshnessTimeMs int64                `json:"minConsumingFreshnessTimeMs"`
}
```

Note that `AggregationResults` and `SelectionResults` are holders for PQL queries.

Meanwhile `ResultTable` is the holder for SQL queries.
`ResultTable` is defined as:

```
// ResultTable is a ResultTable
type ResultTable struct {
	DataSchema RespSchema      `json:"dataSchema"`
	Rows       [][]interface{} `json:"rows"`
}
```

`RespSchema` is defined as:

```
// RespSchema is response schema
type RespSchema struct {
	ColumnDataTypes []string `json:"columnDataTypes"`
	ColumnNames     []string `json:"columnNames"`
}
```

There are multiple functions defined for `ResultTable`, like:

```
func (r ResultTable) GetRowCount() int
func (r ResultTable) GetColumnCount() int
func (r ResultTable) GetColumnName(columnIndex int) string
func (r ResultTable) GetColumnDataType(columnIndex int) string
func (r ResultTable) Get(rowIndex int, columnIndex int) interface{}
func (r ResultTable) GetString(rowIndex int, columnIndex int) string
func (r ResultTable) GetInt(rowIndex int, columnIndex int) int
func (r ResultTable) GetLong(rowIndex int, columnIndex int) int64
func (r ResultTable) GetFloat(rowIndex int, columnIndex int) float32
func (r ResultTable) GetDouble(rowIndex int, columnIndex int) float64
```

Sample Usage is [here](https://github.com/startreedata/pinot-client-go/blob/master/examples/batch-quickstart/main.go#L58)
