---
title: Response Format
layout: default
nav_order: 9
---

# Response Format
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## BrokerResponse

All queries return a `BrokerResponse` struct containing results and query statistics:

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

- **`ResultTable`** — Holds results for SQL queries (recommended).
- **`AggregationResults`** — Holds results for PQL aggregation queries.
- **`SelectionResults`** — Holds results for PQL selection queries.

## ResultTable

`ResultTable` is the primary result holder for SQL queries:

```go
type ResultTable struct {
    DataSchema RespSchema      `json:"dataSchema"`
    Rows       [][]interface{} `json:"rows"`
}
```

### Schema

```go
type RespSchema struct {
    ColumnDataTypes []string `json:"columnDataTypes"`
    ColumnNames     []string `json:"columnNames"`
}
```

### Methods

| Method | Return Type | Description |
|:-------|:-----------|:------------|
| `GetRowCount()` | `int` | Number of result rows |
| `GetColumnCount()` | `int` | Number of columns |
| `GetColumnName(colIndex)` | `string` | Column name at index |
| `GetColumnDataType(colIndex)` | `string` | Column data type at index |
| `Get(row, col)` | `interface{}` | Raw value at position |
| `GetString(row, col)` | `string` | String value |
| `GetInt(row, col)` | `int` | Integer value |
| `GetLong(row, col)` | `int64` | 64-bit integer value |
| `GetFloat(row, col)` | `float32` | 32-bit float value |
| `GetDouble(row, col)` | `float64` | 64-bit float value |

### Example

```go
resp, err := pinotClient.ExecuteSQL("baseballStats",
    "SELECT playerName, teamID, homeRuns FROM baseballStats ORDER BY homeRuns DESC LIMIT 5")
if err != nil {
    log.Fatal(err)
}

table := resp.ResultTable

// Print column headers
for i := 0; i < table.GetColumnCount(); i++ {
    fmt.Printf("%-20s", table.GetColumnName(i))
}
fmt.Println()

// Print rows
for i := 0; i < table.GetRowCount(); i++ {
    fmt.Printf("%-20s%-20s%-20d\n",
        table.GetString(i, 0),
        table.GetString(i, 1),
        table.GetLong(i, 2),
    )
}
```

## Query Statistics

Every response includes execution metadata:

| Field | Description |
|:------|:------------|
| `TimeUsedMs` | Total query execution time in milliseconds |
| `NumDocsScanned` | Number of documents scanned |
| `TotalDocs` | Total documents in the table |
| `NumServersQueried` | Number of servers involved |
| `NumServersResponded` | Number of servers that responded |
| `NumSegmentsQueried` | Segments queried |
| `NumSegmentsProcessed` | Segments processed |
| `NumSegmentsMatched` | Segments with matching data |
| `NumEntriesScannedInFilter` | Entries scanned during filtering |
| `NumEntriesScannedPostFilter` | Entries scanned after filtering |
| `Exceptions` | Any errors during query execution |

## Error Handling

Check the `Exceptions` field for query-level errors:

```go
resp, err := pinotClient.ExecuteSQL("baseballStats", "SELECT ...")
if err != nil {
    // Transport or connection error
    log.Fatal(err)
}

if len(resp.Exceptions) > 0 {
    // Query execution error from broker
    for _, ex := range resp.Exceptions {
        log.Printf("Exception: %v", ex)
    }
}
```
