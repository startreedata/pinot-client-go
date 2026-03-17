---
title: Configuration
layout: default
nav_order: 8
---

# Configuration
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## ClientConfig

`ClientConfig` is the main configuration struct for the Pinot client. Pass it to `pinot.NewWithConfig()` for full control over client behavior.

```go
pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
    // Choose one connection method:
    BrokerList:       []string{"localhost:8000"},
    // ZkConfig:      &pinot.ZookeeperConfig{...},
    // ControllerConfig: &pinot.ControllerConfig{...},

    // Optional settings:
    ExtraHTTPHeader: map[string]string{
        "Authorization": "Bearer <token>",
    },
    HTTPTimeout: 5 * time.Second,
    GrpcConfig:  &pinot.GrpcConfig{...},
})
```

## HTTP Timeout

By default, the client uses Go's default HTTP timeout (no timeout). Configure a timeout for Pinot queries:

```go
pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
    BrokerList:  []string{"localhost:8000"},
    HTTPTimeout: 300 * time.Millisecond,
})
```

## Extra HTTP Headers

Add custom headers to all broker query API requests:

```go
pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
    BrokerList: []string{"localhost:8000"},
    ExtraHTTPHeader: map[string]string{
        "Authorization": "Bearer <token>",
        "X-Custom":      "value",
    },
})
```

## ZookeeperConfig

Configure Zookeeper-based broker discovery:

```go
ZkConfig: &pinot.ZookeeperConfig{
    ZookeeperPath:     zkPath,
    PathPrefix:        strings.Join([]string{zkPathPrefix, pinotCluster}, "/"),
    SessionTimeoutSec: 60,
}
```

| Field | Type | Description |
|:------|:-----|:------------|
| `ZookeeperPath` | `string` | Zookeeper connection path |
| `PathPrefix` | `string` | Path prefix for the Pinot cluster |
| `SessionTimeoutSec` | `int` | Zookeeper session timeout in seconds |

## ControllerConfig

Configure controller-based broker discovery:

```go
ControllerConfig: &pinot.ControllerConfig{
    ControllerAddress: "localhost:9000",
    UpdateFreqMs:      500,
    ExtraControllerAPIHeaders: map[string]string{
        "Authorization": "Bearer <token>",
    },
}
```

| Field | Type | Description |
|:------|:-----|:------------|
| `ControllerAddress` | `string` | Controller host and port |
| `UpdateFreqMs` | `int` | Broker refresh frequency in ms (default: 1000) |
| `ExtraControllerAPIHeaders` | `map[string]string` | Extra headers for controller API calls |

## GrpcConfig

Configure gRPC transport. See [gRPC Transport](grpc) for full details.

```go
GrpcConfig: &pinot.GrpcConfig{
    Encoding:     "JSON",
    Compression:  "ZSTD",
    BlockRowSize: 10000,
    Timeout:      5 * time.Second,
    TLSConfig: &pinot.GrpcTLSConfig{
        Enabled:    true,
        CACertPath: "/path/to/ca.pem",
    },
}
```

| Field | Type | Description |
|:------|:-----|:------------|
| `Encoding` | `string` | `"JSON"` or `"ARROW"` |
| `Compression` | `string` | `ZSTD`, `LZ4`, `DEFLATE`, `GZIP`, `SNAPPY` |
| `BlockRowSize` | `int` | Rows per response block |
| `Timeout` | `time.Duration` | gRPC query timeout |
| `TLSConfig` | `*GrpcTLSConfig` | TLS settings |
