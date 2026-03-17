---
title: Getting Started
layout: default
nav_order: 2
---

# Getting Started
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Installation

Install the Pinot Go client using `go get`:

```sh
go get github.com/startreedata/pinot-client-go
```

## Prerequisites

You need a running Apache Pinot cluster. Follow the [Pinot Quickstart](https://docs.pinot.apache.org/basics/getting-started/running-pinot-locally) guide to install and start Pinot locally.

### Start Pinot Batch Quickstart

```sh
bin/quick-start-batch.sh
```

This starts a local Pinot cluster with sample data (baseballStats) that you can use to test the client.

## Running the Examples

Clone the repository and run a quickstart example:

```sh
git clone git@github.com:startreedata/pinot-client-go.git
cd pinot-client-go
```

### Batch Quickstart

```sh
go build ./examples/batch-quickstart
./batch-quickstart
```

### JSON Index Quickstart

Start the JSON index batch quickstart:

```sh
bin/quick-start-json-index-batch.sh
```

Then build and run:

```sh
go build ./examples/json-batch-quickstart
./json-batch-quickstart
```

### Multi-Stage Engine Quickstart

```sh
go build ./examples/multistage-quickstart
./multistage-quickstart
```

## Your First Query

Here's a minimal example to connect and query Pinot:

```go
package main

import (
    "fmt"
    "log"

    pinot "github.com/startreedata/pinot-client-go/pinot"
)

func main() {
    // Connect to a Pinot broker
    pinotClient, err := pinot.NewFromBrokerList([]string{"localhost:8000"})
    if err != nil {
        log.Fatal(err)
    }

    // Execute a SQL query
    resp, err := pinotClient.ExecuteSQL(
        "baseballStats",
        "SELECT count(*) AS cnt, sum(homeRuns) AS sum_homeRuns FROM baseballStats GROUP BY teamID LIMIT 10",
    )
    if err != nil {
        log.Fatal(err)
    }

    // Print query statistics
    fmt.Printf("Query time: %d ms, scanned docs: %d, total docs: %d\n",
        resp.TimeUsedMs, resp.NumDocsScanned, resp.TotalDocs)

    // Iterate over results
    for i := 0; i < resp.ResultTable.GetRowCount(); i++ {
        fmt.Printf("Row %d: count=%d, sum_homeRuns=%d\n",
            i,
            resp.ResultTable.GetLong(i, 0),
            resp.ResultTable.GetLong(i, 1),
        )
    }
}
```

## Available Examples

| Example | Description |
|:--------|:------------|
| `batch-quickstart` | Basic SQL queries against batch data |
| `json-batch-quickstart` | JSON index queries |
| `multistage-quickstart` | Multi-stage engine queries |
| `prepared-statement-example` | Parameterized queries |
| `gorm-example` | GORM dialector integration |
| `grpc-broker-client` | gRPC broker queries |
| `pinot-client-withconfig` | Custom ClientConfig |
| `pinot-client-with-config-and-http-client` | Custom HTTP client |
| `pinot-live-demo` | Live demo integration |
