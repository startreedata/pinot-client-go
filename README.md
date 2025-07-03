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

```sh
bin/quick-start-batch.sh
```

Check out Client library Github Repo

```sh
git clone git@github.com:startreedata/pinot-client-go.git
cd pinot-client-go
```

Build and run the example application to query from Pinot Batch Quickstart

```sh
go build ./examples/batch-quickstart
./batch-quickstart
```

## Pinot Json Index QuickStart

Please follow this [Pinot Quickstart](https://docs.pinot.apache.org/basics/getting-started/running-pinot-locally) link to install and start Pinot json batch quickstart locally.

```sh
bin/quick-start-json-index-batch.sh
```

Check out Client library Github Repo

```sh
git clone git@github.com:startreedata/pinot-client-go.git
cd pinot-client-go
```

Build and run the example application to query from Pinot Json Batch Quickstart

```sh
go build ./examples/json-batch-quickstart
./json-batch-quickstart
```

# Usage

## Create a Pinot Connection

Pinot client could be initialized through:

1. Zookeeper Path.

```go
pinotClient, err := pinot.NewFromZookeeper([]string{"localhost:2123"}, "", "QuickStartCluster")
```

2. Controller address.

```go
pinotClient, err := pinot.NewFromController("localhost:9000")
```

When the controller-based broker selector is used, the client will periodically fetch the table-to-broker mapping from the controller API. When using `http` scheme, the `http://` controller address prefix is optional.

3. A list of broker addresses.

- For HTTP
  Default scheme is HTTP if not specified.

```go
pinotClient, err := pinot.NewFromBrokerList([]string{"localhost:8000"})
```

- For HTTPS
  Scheme is required to be part of the URI.

```go
pinotClient, err := pinot.NewFromBrokerList([]string{"https://pinot-broker.pinot.live"})
```

4. ClientConfig

Via Zookeeper path:

```go
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

```go
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

```go
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

```go
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

```sh
go build ./examples/multistage-quickstart
./multistage-quickstart
```

Code snippet:

```go
pinotClient, err := pinot.NewFromZookeeper([]string{"localhost:2123"}, "", "QuickStartCluster")
if err != nil {
	log.Error(err)
}
pinotClient.UseMultistageEngine(true)
```

## Using PreparedStatement

PreparedStatement provides a convenient and efficient way to execute parameterized queries. It's similar to Java's PreparedStatement, offering type safety, parameter validation, and reusability.

Please see this [example](https://github.com/startreedata/pinot-client-go/blob/master/examples/prepared-statement-example/main.go) for your reference.

How to run it:

```sh
go build ./examples/prepared-statement-example
./prepared-statement-example
```

### Basic PreparedStatement Usage

```go
// Create a connection
pinotClient, err := pinot.NewFromZookeeper([]string{"localhost:2123"}, "", "QuickStartCluster")
if err != nil {
    log.Error(err)
}

// Create a prepared statement
stmt, err := pinotClient.Prepare("baseballStats", "SELECT playerName, homeRuns FROM baseballStats WHERE teamID = ? AND yearID = ? ORDER BY homeRuns DESC LIMIT ?")
if err != nil {
    log.Error(err)
}
defer stmt.Close() // Always close the statement when done

// Set parameters (1-based indexing like Java PreparedStatement)
err = stmt.SetString(1, "SFN")  // teamID
if err != nil {
    log.Error(err)
}
err = stmt.SetInt(2, 2000)      // yearID
if err != nil {
    log.Error(err)
}
err = stmt.SetInt(3, 10)        // LIMIT
if err != nil {
    log.Error(err)
}

// Execute the query
response, err := stmt.Execute()
if err != nil {
    log.Error(err)
}

// Process results
for i := 0; i < response.ResultTable.GetRowCount(); i++ {
    playerName := response.ResultTable.GetString(i, 0)
    homeRuns := response.ResultTable.GetLong(i, 1)
    log.Printf("Player: %s, Home Runs: %d", playerName, homeRuns)
}
```

### PreparedStatement with ExecuteWithParams

For one-time execution, you can use the `ExecuteWithParams` convenience method:

```go
stmt, err := pinotClient.Prepare("baseballStats", "SELECT COUNT(*) as cnt FROM baseballStats WHERE teamID = ? AND yearID >= ?")
if err != nil {
    log.Error(err)
}
defer stmt.Close()

// Execute with parameters in one call
response, err := stmt.ExecuteWithParams("NYA", 2000)
if err != nil {
    log.Error(err)
}

count := response.ResultTable.GetLong(0, 0)
log.Printf("Count: %d", count)
```

### Reusing PreparedStatement

PreparedStatements can be reused with different parameters for better performance:

```go
stmt, err := pinotClient.Prepare("baseballStats", "SELECT COUNT(*) as playerCount FROM baseballStats WHERE teamID = ?")
if err != nil {
    log.Error(err)
}
defer stmt.Close()

teams := []string{"NYA", "BOS", "LAA", "SFN"}
for _, team := range teams {
    // Clear previous parameters
    err = stmt.ClearParameters()
    if err != nil {
        log.Error(err)
    }
    
    // Set new parameter
    err = stmt.SetString(1, team)
    if err != nil {
        log.Error(err)
    }
    
    // Execute
    response, err := stmt.Execute()
    if err != nil {
        log.Error(err)
    }
    
    count := response.ResultTable.GetLong(0, 0)
    log.Printf("Team %s: %d players", team, count)
}
```

### Supported Parameter Types

PreparedStatement supports various parameter types with dedicated setter methods:

```go
stmt, err := pinotClient.Prepare("baseballStats", 
    "SELECT * FROM baseballStats WHERE yearID = ? AND homeRuns >= ? AND battingAvg > ? AND active = ?")
if err != nil {
    log.Error(err)
}
defer stmt.Close()

// Type-specific setters
err = stmt.SetInt(1, 2001)           // int
err = stmt.SetInt64(2, 25)           // int64  
err = stmt.SetFloat64(3, 0.300)      // float64
err = stmt.SetBool(4, true)          // bool
err = stmt.SetString(5, "player")    // string

// Generic setter for any supported type
err = stmt.Set(1, 2001)              // Automatically detects type
err = stmt.Set(2, int64(25))         // Explicit type conversion
err = stmt.Set(3, 0.300)             // float64
err = stmt.Set(4, true)              // bool

// Execute
response, err := stmt.Execute()
```

### PreparedStatement with Complex Queries

PreparedStatement works well with complex queries including aggregations, joins, and subqueries:

```go
// Complex aggregation query
stmt, err := pinotClient.Prepare("baseballStats", `
    SELECT teamID, 
           COUNT(*) as playerCount,
           SUM(homeRuns) as totalHomeRuns,
           AVG(battingAvg) as avgBattingAvg
    FROM baseballStats 
    WHERE yearID BETWEEN ? AND ? 
      AND homeRuns >= ? 
    GROUP BY teamID 
    HAVING COUNT(*) > ?
    ORDER BY totalHomeRuns DESC 
    LIMIT ?`)
if err != nil {
    log.Error(err)
}
defer stmt.Close()

// Execute with multiple parameters
response, err := stmt.ExecuteWithParams(2000, 2010, 10, 5, 10)
if err != nil {
    log.Error(err)
}

// Process aggregated results
for i := 0; i < response.ResultTable.GetRowCount(); i++ {
    teamID := response.ResultTable.GetString(i, 0)
    playerCount := response.ResultTable.GetLong(i, 1)
    totalHomeRuns := response.ResultTable.GetLong(i, 2)
    avgBattingAvg := response.ResultTable.GetDouble(i, 3)
    
    log.Printf("Team: %s, Players: %d, Total HRs: %d, Avg BA: %.3f", 
        teamID, playerCount, totalHomeRuns, avgBattingAvg)
}
```

### PreparedStatement Methods

PreparedStatement provides the following methods:

```go
type PreparedStatement interface {
    // Parameter setting methods
    SetString(parameterIndex int, value string) error
    SetInt(parameterIndex int, value int) error
    SetInt64(parameterIndex int, value int64) error
    SetFloat64(parameterIndex int, value float64) error
    SetBool(parameterIndex int, value bool) error
    Set(parameterIndex int, value interface{}) error
    
    // Execution methods
    Execute() (*BrokerResponse, error)
    ExecuteWithParams(params ...interface{}) (*BrokerResponse, error)
    
    // Utility methods
    GetQuery() string
    GetParameterCount() int
    ClearParameters() error
    Close() error
}
```

### Best Practices

1. **Always close PreparedStatements**: Use `defer stmt.Close()` to ensure proper resource cleanup
2. **Reuse PreparedStatements**: For repeated queries with different parameters, reuse the same PreparedStatement
3. **Use type-specific setters**: Use `SetString()`, `SetInt()`, etc. for better type safety
4. **Handle errors properly**: Always check for errors when setting parameters and executing queries
5. **Clear parameters when reusing**: Use `ClearParameters()` when reusing statements with different parameter sets

### Thread Safety

PreparedStatement is thread-safe and can be used concurrently from multiple goroutines. However, parameter setting and execution should be coordinated to avoid race conditions in your application logic.

## Response Format

Query Response is defined as the struct of following:

```go
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

```go
// ResultTable is a ResultTable
type ResultTable struct {
	DataSchema RespSchema      `json:"dataSchema"`
	Rows       [][]interface{} `json:"rows"`
}
```

`RespSchema` is defined as:

```go
// RespSchema is response schema
type RespSchema struct {
	ColumnDataTypes []string `json:"columnDataTypes"`
	ColumnNames     []string `json:"columnNames"`
}
```

There are multiple functions defined for `ResultTable`, like:

```go
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

# How to release

## Tag and publish the release in Github

Tag the version:

```sh
git tag -a v0.5.0 -m "v0.5.0"
git push origin v0.5.0
```

Go to [Github Release](https://github.com/startreedata/pinot-client-go/releases) and create a new release with the tag, e.g. [Pinot Golang Client v0.5.0](https://github.com/startreedata/pinot-client-go/releases/tag/v0.5.0)

## Publish the release in Go Modules

The published Release will be available in [Go Modules](https://pkg.go.dev/github.com/startreedata/pinot-client-go).

If not available, go to the corresponding new version page (https://pkg.go.dev/github.com/startreedata/pinot-client-go@v0.5.0) and click on the "Request New Version" button.

