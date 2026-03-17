---
title: Connection
layout: default
nav_order: 3
---

# Connection
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

The Pinot client can be initialized through several methods depending on your deployment setup.

## From Broker List

Connect directly to one or more Pinot brokers. This is the simplest method when you know the broker addresses.

### HTTP (default)

```go
pinotClient, err := pinot.NewFromBrokerList([]string{"localhost:8000"})
```

### HTTPS

Include the scheme in the URI:

```go
pinotClient, err := pinot.NewFromBrokerList([]string{"https://pinot-broker.pinot.live"})
```

## From Zookeeper

Connect via Zookeeper for automatic broker discovery:

```go
pinotClient, err := pinot.NewFromZookeeper(
    []string{"localhost:2123"}, // Zookeeper hosts
    "",                          // Path prefix
    "QuickStartCluster",         // Pinot cluster name
)
```

The client automatically discovers and tracks available brokers through Zookeeper.

## From Controller

Connect via the Pinot Controller API:

```go
pinotClient, err := pinot.NewFromController("localhost:9000")
```

When using the controller-based broker selector, the client periodically fetches the table-to-broker mapping from the controller API. The `http://` prefix is optional when using HTTP scheme.

## Using ClientConfig

For advanced configuration, use `NewWithConfig` with a `ClientConfig` struct.

### With Zookeeper

```go
pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
    ZkConfig: &pinot.ZookeeperConfig{
        ZookeeperPath:     zkPath,
        PathPrefix:        strings.Join([]string{zkPathPrefix, pinotCluster}, "/"),
        SessionTimeoutSec: defaultZkSessionTimeoutSec,
    },
    ExtraHTTPHeader: map[string]string{
        "extra-header": "value",
    },
})
```

### With Controller

```go
pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
    ControllerConfig: &pinot.ControllerConfig{
        ControllerAddress: "localhost:9000",
        // Frequency of broker data refresh in milliseconds — defaults to 1000ms
        UpdateFreqMs: 500,
        // Additional HTTP headers for controller API requests
        ExtraControllerAPIHeaders: map[string]string{
            "header": "val",
        },
    },
    ExtraHTTPHeader: map[string]string{
        "extra-header": "value",
    },
})
```

## Connection Method Comparison

| Method | Discovery | Use Case |
|:-------|:----------|:---------|
| `NewFromBrokerList` | Static | Known broker addresses, simple setups |
| `NewFromZookeeper` | Dynamic | Production clusters with Zookeeper |
| `NewFromController` | Dynamic | Production clusters using Controller API |
| `NewWithConfig` | Any | Advanced configuration needs |
