---
title: Querying
layout: default
nav_order: 4
---

# Querying
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Executing SQL Queries

Use `ExecuteSQL` to run SQL queries against Pinot:

```go
pinotClient, err := pinot.NewFromZookeeper([]string{"localhost:2123"}, "", "QuickStartCluster")
if err != nil {
    log.Fatal(err)
}

resp, err := pinotClient.ExecuteSQL(
    "baseballStats",
    "SELECT count(*) AS cnt, sum(homeRuns) AS sum_homeRuns FROM baseballStats GROUP BY teamID LIMIT 10",
)
if err != nil {
    log.Fatal(err)
}

log.Printf("Query time: %d ms, scanned docs: %d, total docs: %d",
    resp.TimeUsedMs, resp.NumDocsScanned, resp.TotalDocs)
```

## Multi-Stage Engine

Pinot supports a multi-stage query engine for more complex queries including JOINs. Enable it on the client:

```go
pinotClient, err := pinot.NewFromZookeeper([]string{"localhost:2123"}, "", "QuickStartCluster")
if err != nil {
    log.Fatal(err)
}

pinotClient.UseMultistageEngine(true)

// Now queries can use multi-stage features like JOINs
resp, err := pinotClient.ExecuteSQL("baseballStats", "SELECT ...")
```

## Reading Results

SQL query results are returned in a `ResultTable` within the `BrokerResponse`:

```go
resp, err := pinotClient.ExecuteSQL("baseballStats", "SELECT playerName, homeRuns FROM baseballStats LIMIT 5")
if err != nil {
    log.Fatal(err)
}

table := resp.ResultTable

// Get dimensions
rowCount := table.GetRowCount()
colCount := table.GetColumnCount()

// Get column metadata
for i := 0; i < colCount; i++ {
    fmt.Printf("Column %d: %s (%s)\n", i, table.GetColumnName(i), table.GetColumnDataType(i))
}

// Read typed values
for i := 0; i < rowCount; i++ {
    name := table.GetString(i, 0)
    homeRuns := table.GetLong(i, 1)
    fmt.Printf("%s: %d\n", name, homeRuns)
}
```

### Available Type Accessors

| Method | Return Type | Description |
|:-------|:-----------|:------------|
| `Get(row, col)` | `interface{}` | Raw value |
| `GetString(row, col)` | `string` | String value |
| `GetInt(row, col)` | `int` | Integer value |
| `GetLong(row, col)` | `int64` | 64-bit integer value |
| `GetFloat(row, col)` | `float32` | 32-bit float value |
| `GetDouble(row, col)` | `float64` | 64-bit float value |

## Query Statistics

Every `BrokerResponse` includes query execution statistics:

```go
resp, _ := pinotClient.ExecuteSQL("baseballStats", "SELECT ...")

fmt.Printf("Time used: %d ms\n", resp.TimeUsedMs)
fmt.Printf("Docs scanned: %d\n", resp.NumDocsScanned)
fmt.Printf("Total docs: %d\n", resp.TotalDocs)
fmt.Printf("Segments queried: %d\n", resp.NumSegmentsQueried)
fmt.Printf("Segments matched: %d\n", resp.NumSegmentsMatched)
fmt.Printf("Servers queried: %d\n", resp.NumServersQueried)
fmt.Printf("Servers responded: %d\n", resp.NumServersResponded)
```

## Query Tracing

Enable tracing to get detailed execution information from brokers:

```go
pinotClient.OpenTrace()
defer pinotClient.CloseTrace()

resp, err := pinotClient.ExecuteSQL("baseballStats", "SELECT ...")
// resp.TraceInfo contains trace details
```
