# Pinot Client GO

[![Go 1.19](https://img.shields.io/badge/go-1.19-blue.svg)](https://golang.org/dl/#go1.19)
[![GoDoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/startreedata/pinot-client-go)
[![Build Status](https://github.com/startreedata/pinot-client-go/actions/workflows/tests.yml/badge.svg)](https://github.com/startreedata/pinot-client-go/actions/workflows/tests.yml)
[![Coverage Status](https://coveralls.io/repos/github/startreedata/pinot-client-go/badge.svg?branch=master)](https://coveralls.io/github/startreedata/pinot-client-go?branch=master)

![image](https://user-images.githubusercontent.com/1202120/116982228-63315900-ac7d-11eb-96e5-01a04ef7d737.png)

Applications can use this golang client library to query Apache Pinot.

# Examples

## Local Pinot test

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

## Pinot Json Index QuickStart

Please follow this [Pinot Quickstart](https://docs.pinot.apache.org/basics/getting-started/running-pinot-locally) link to install and start Pinot json batch quickstart locally.

```
bin/quick-start-json-index-batch.sh
```

Check out Client library Github Repo

```
git clone git@github.com:startreedata/pinot-client-go.git
cd pinot-client-go
```

Build and run the example application to query from Pinot Json Batch Quickstart

```
go build ./examples/json-batch-quickstart
./json-batch-quickstart
```

# Usage

## Create a Pinot Connection

Pinot client could be initialized through:

1. Zookeeper Path.

```
pinotClient, err := pinot.NewFromZookeeper([]string{"localhost:2123"}, "", "QuickStartCluster")
```

2. Controller address.

```
pinotClient, err := pinot.NewFromController("localhost:9000")
```

When the controller-based broker selector is used, the client will periodically fetch the table-to-broker mapping from the controller API. When using `http` scheme, the `http://` controller address prefix is optional.

3. A list of broker addresses.

- For HTTP
  Default scheme is HTTP if not specified.

```
pinotClient, err := pinot.NewFromBrokerList([]string{"localhost:8000"})
```

- For HTTPS
  Scheme is required to be part of the URI.

```
pinotClient, err := pinot.NewFromBrokerList([]string{"https://pinot-broker.pinot.live"})
```

4. ClientConfig

Via Zookeeper path:

```
pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
	ZkConfig: &pinot.ZookeeperConfig{
		ZookeeperPath:     zkPath,
		PathPrefix:        strings.Join([]string{zkPathPrefix, pinotCluster}, "/"),
		SessionTimeoutSec: defaultZkSessionTimeoutSec,
	},
	// additional header added to Broker Query API requests
    ExtraHTTPHeader: map[string]string{
        "extra-header":"value",
    },
})
```

Via controller address:

```
pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
	ControllerConfig: &pinot.ControllerConfig{
		ControllerAddress: "localhost:9000",
		// Frequency of broker data refresh in milliseconds via controller API - defaults to 1000ms
		UpdateFreqMs: 500,
		// Additional HTTP headers to include in the controller API request
		ExtraControllerAPIHeaders: map[string]string{
			"header": "val",
		},
	},
	// additional header added to Broker Query API requests
	ExtraHTTPHeader: map[string]string{
		"extra-header": "value",
	},
})
```

### Add HTTP timeout for Pinot Queries

By Default this client uses golang's default http timeout, which is "No TImeout". If you want pinot queries to timeout within given time, add `HTTPTimeout` in `ClientConfig`

```
pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
	ZkConfig: &pinot.ZookeeperConfig{
		ZookeeperPath:     zkPath,
		PathPrefix:        strings.Join([]string{zkPathPrefix, pinotCluster}, "/"),
		SessionTimeoutSec: defaultZkSessionTimeoutSec,
	},
	// additional header added to Broker Query API requests
    ExtraHTTPHeader: map[string]string{
        "extra-header":"value",
    },
	// optional HTTP timeout parameter for Pinot Queries.
	HTTPTimeout: 300 * time.Millisecond,
})
```

## Query Pinot

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

## Query Pinot with Multi-Stage Engine

Please see this [example](https://github.com/startreedata/pinot-client-go/blob/master/examples/multistage-quickstart/main.go) for your reference.

How to run it:

```
go build ./examples/multistage-quickstart
./multistage-quickstart
```

Code snippet:

```
pinotClient, err := pinot.NewFromZookeeper([]string{"localhost:2123"}, "", "QuickStartCluster")
if err != nil {
	log.Error(err)
}
pinotClient.UseMultistageEngine(true)
```

## Response Format

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
