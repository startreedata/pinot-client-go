---
title: Home
layout: home
nav_order: 1
---

# Pinot Client Go

[![Go 1.24](https://img.shields.io/badge/go-1.24-blue.svg)](https://golang.org/dl/#go1.24)
[![GoDoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/startreedata/pinot-client-go)
[![Build Status](https://github.com/startreedata/pinot-client-go/actions/workflows/tests.yml/badge.svg)](https://github.com/startreedata/pinot-client-go/actions/workflows/tests.yml)
[![Coverage Status](https://coveralls.io/repos/github/startreedata/pinot-client-go/badge.svg?branch=master)](https://coveralls.io/github/startreedata/pinot-client-go?branch=master)

**pinot-client-go** is the official Go client library for [Apache Pinot](https://pinot.apache.org/), a real-time distributed OLAP database designed for low-latency, high-throughput analytics.

## Features

- **Multiple connection methods** — Connect via Zookeeper, Controller, or direct broker list
- **HTTP and gRPC transports** — Choose the transport that fits your use case
- **Prepared statements** — Type-safe parameterized queries with reusable statements
- **GORM integration** — Use familiar ORM patterns for read-only Pinot queries
- **Multi-stage query engine** — Support for Pinot's advanced multi-stage execution
- **Flexible configuration** — Custom HTTP clients, timeouts, headers, and TLS

## Quick Install

```sh
go get github.com/startreedata/pinot-client-go
```

## Quick Example

```go
package main

import (
    "fmt"
    "log"

    pinot "github.com/startreedata/pinot-client-go/pinot"
)

func main() {
    pinotClient, err := pinot.NewFromBrokerList([]string{"localhost:8000"})
    if err != nil {
        log.Fatal(err)
    }

    resp, err := pinotClient.ExecuteSQL(
        "baseballStats",
        "SELECT playerName, homeRuns FROM baseballStats ORDER BY homeRuns DESC LIMIT 5",
    )
    if err != nil {
        log.Fatal(err)
    }

    for i := 0; i < resp.ResultTable.GetRowCount(); i++ {
        fmt.Printf("%s: %d\n",
            resp.ResultTable.GetString(i, 0),
            resp.ResultTable.GetLong(i, 1),
        )
    }
}
```

## Next Steps

- [Getting Started](getting-started) — Set up your environment and run your first query
- [Connection](connection) — Learn about different connection methods
- [Querying](querying) — Execute SQL queries against Pinot
- [Prepared Statements](prepared-statements) — Use parameterized queries
- [gRPC Transport](grpc) — Configure gRPC-based broker communication
- [GORM Integration](gorm) — Use GORM ORM with Pinot
- [Configuration](configuration) — Advanced configuration options
- [Response Format](response) — Understand query response structures
