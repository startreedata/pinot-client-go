---
title: gRPC Transport
layout: default
nav_order: 6
---

# gRPC Transport
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

Pinot brokers expose a gRPC query endpoint when configured with `pinot.broker.grpc.port`. The Go client supports gRPC as an alternative to the default HTTP/JSON transport.

See the [Pinot gRPC Broker API docs](https://docs.pinot.apache.org/users/api/broker-grpc-api) for server-side configuration details.

## Basic gRPC Configuration

```go
pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
    BrokerList: []string{"localhost:8010"},
    GrpcConfig: &pinot.GrpcConfig{
        Encoding:     "JSON",
        Compression:  "ZSTD",
        BlockRowSize: 10000,
        Timeout:      5 * time.Second,
    },
})
```

## Configuration Options

| Option | Type | Description |
|:-------|:-----|:------------|
| `Encoding` | `string` | Response encoding format: `"JSON"` or `"ARROW"` |
| `Compression` | `string` | Compression algorithm for responses |
| `BlockRowSize` | `int` | Number of rows per response block |
| `Timeout` | `time.Duration` | Query timeout for gRPC calls |
| `TLSConfig` | `*GrpcTLSConfig` | TLS configuration for secure connections |

## Supported Compression

- `ZSTD`
- `LZ4`
- `DEFLATE`
- `GZIP`
- `SNAPPY`

## TLS Configuration

Enable TLS for secure gRPC connections:

```go
pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
    BrokerList: []string{"pinot-broker:8010"},
    GrpcConfig: &pinot.GrpcConfig{
        Encoding:    "JSON",
        Compression: "ZSTD",
        Timeout:     5 * time.Second,
        TLSConfig: &pinot.GrpcTLSConfig{
            Enabled:    true,
            CACertPath: "/path/to/ca.pem",
        },
    },
})
```

## Encoding Formats

### JSON

The default encoding. Results are returned as JSON and deserialized into Go types.

```go
GrpcConfig: &pinot.GrpcConfig{
    Encoding: "JSON",
}
```

### Arrow

Apache Arrow columnar format for high-performance data transfer. Useful for large result sets.

```go
GrpcConfig: &pinot.GrpcConfig{
    Encoding: "ARROW",
}
```

## Usage

Once configured with gRPC, query execution is identical to HTTP:

```go
resp, err := pinotClient.ExecuteSQL(
    "baseballStats",
    "SELECT playerName, homeRuns FROM baseballStats ORDER BY homeRuns DESC LIMIT 10",
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
```
