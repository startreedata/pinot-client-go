---
title: Prepared Statements
layout: default
nav_order: 5
---

# Prepared Statements
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

`PreparedStatement` provides a convenient and efficient way to execute parameterized queries. It offers type safety, parameter validation, and reusability — similar to Java's `PreparedStatement`.

## Basic Usage

```go
// Create a connection
pinotClient, err := pinot.NewFromZookeeper([]string{"localhost:2123"}, "", "QuickStartCluster")
if err != nil {
    log.Fatal(err)
}

// Create a prepared statement with ? placeholders
stmt, err := pinotClient.Prepare(
    "baseballStats",
    "SELECT playerName, homeRuns FROM baseballStats WHERE teamID = ? AND yearID = ? ORDER BY homeRuns DESC LIMIT ?",
)
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Set parameters (1-based indexing)
stmt.SetString(1, "SFN")   // teamID
stmt.SetInt(2, 2000)        // yearID
stmt.SetInt(3, 10)          // LIMIT

// Execute the query
response, err := stmt.Execute()
if err != nil {
    log.Fatal(err)
}

// Process results
for i := 0; i < response.ResultTable.GetRowCount(); i++ {
    playerName := response.ResultTable.GetString(i, 0)
    homeRuns := response.ResultTable.GetLong(i, 1)
    log.Printf("Player: %s, Home Runs: %d", playerName, homeRuns)
}
```

## ExecuteWithParams

For one-time execution, use `ExecuteWithParams` to set parameters and execute in one call:

```go
stmt, err := pinotClient.Prepare(
    "baseballStats",
    "SELECT COUNT(*) AS cnt FROM baseballStats WHERE teamID = ? AND yearID >= ?",
)
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

response, err := stmt.ExecuteWithParams("NYA", 2000)
if err != nil {
    log.Fatal(err)
}

count := response.ResultTable.GetLong(0, 0)
log.Printf("Count: %d", count)
```

## Reusing Statements

Reuse a `PreparedStatement` with different parameters for better performance:

```go
stmt, err := pinotClient.Prepare(
    "baseballStats",
    "SELECT COUNT(*) AS playerCount FROM baseballStats WHERE teamID = ?",
)
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

teams := []string{"NYA", "BOS", "LAA", "SFN"}
for _, team := range teams {
    stmt.ClearParameters()
    stmt.SetString(1, team)

    response, err := stmt.Execute()
    if err != nil {
        log.Fatal(err)
    }

    count := response.ResultTable.GetLong(0, 0)
    log.Printf("Team %s: %d players", team, count)
}
```

## Supported Parameter Types

| Method | Go Type | Example |
|:-------|:--------|:--------|
| `SetString(index, value)` | `string` | `stmt.SetString(1, "NYA")` |
| `SetInt(index, value)` | `int` | `stmt.SetInt(1, 2001)` |
| `SetInt64(index, value)` | `int64` | `stmt.SetInt64(1, 25)` |
| `SetFloat64(index, value)` | `float64` | `stmt.SetFloat64(1, 0.300)` |
| `SetBool(index, value)` | `bool` | `stmt.SetBool(1, true)` |
| `Set(index, value)` | `interface{}` | `stmt.Set(1, 2001)` |

The generic `Set` method automatically detects the type of the value.

## Complex Queries

PreparedStatements work with aggregations, GROUP BY, HAVING, and other complex SQL:

```go
stmt, err := pinotClient.Prepare("baseballStats", `
    SELECT teamID,
           COUNT(*) AS playerCount,
           SUM(homeRuns) AS totalHomeRuns,
           AVG(battingAvg) AS avgBattingAvg
    FROM baseballStats
    WHERE yearID BETWEEN ? AND ?
      AND homeRuns >= ?
    GROUP BY teamID
    HAVING COUNT(*) > ?
    ORDER BY totalHomeRuns DESC
    LIMIT ?`)
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

response, err := stmt.ExecuteWithParams(2000, 2010, 10, 5, 10)
if err != nil {
    log.Fatal(err)
}

for i := 0; i < response.ResultTable.GetRowCount(); i++ {
    teamID := response.ResultTable.GetString(i, 0)
    playerCount := response.ResultTable.GetLong(i, 1)
    totalHomeRuns := response.ResultTable.GetLong(i, 2)
    avgBattingAvg := response.ResultTable.GetDouble(i, 3)

    log.Printf("Team: %s, Players: %d, Total HRs: %d, Avg BA: %.3f",
        teamID, playerCount, totalHomeRuns, avgBattingAvg)
}
```

## PreparedStatement API

```go
type PreparedStatement interface {
    // Parameter setters (1-based indexing)
    SetString(parameterIndex int, value string) error
    SetInt(parameterIndex int, value int) error
    SetInt64(parameterIndex int, value int64) error
    SetFloat64(parameterIndex int, value float64) error
    SetBool(parameterIndex int, value bool) error
    Set(parameterIndex int, value interface{}) error

    // Execution
    Execute() (*BrokerResponse, error)
    ExecuteWithParams(params ...interface{}) (*BrokerResponse, error)

    // Utilities
    GetQuery() string
    GetParameterCount() int
    ClearParameters() error
    Close() error
}
```

## Best Practices

1. **Always close statements** — Use `defer stmt.Close()` for proper resource cleanup.
2. **Reuse statements** — For repeated queries with different parameters, reuse the same `PreparedStatement`.
3. **Use type-specific setters** — Prefer `SetString()`, `SetInt()`, etc. over `Set()` for better type safety.
4. **Clear parameters when reusing** — Call `ClearParameters()` before setting new values on a reused statement.
5. **Handle errors** — Always check errors from parameter setting and execution.

## Thread Safety

`PreparedStatement` is thread-safe and can be used concurrently from multiple goroutines. Coordinate parameter setting and execution in your application logic to avoid race conditions.
