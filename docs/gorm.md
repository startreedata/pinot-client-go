---
title: GORM Integration
layout: default
nav_order: 7
---

# GORM Integration
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

The `gormpinot` package provides a read-only [GORM](https://gorm.io/) dialector for Apache Pinot, allowing you to use familiar ORM patterns to query Pinot.

## Installation

The GORM dialector is included in the pinot-client-go module:

```sh
go get github.com/startreedata/pinot-client-go
```

You also need GORM:

```sh
go get gorm.io/gorm
```

## Basic Usage

```go
import (
    pinot "github.com/startreedata/pinot-client-go/pinot"
    "github.com/startreedata/pinot-client-go/gormpinot"
    "gorm.io/gorm"
)

// Create a Pinot connection
conn, err := pinot.NewFromBrokerList([]string{"localhost:8000"})
if err != nil {
    log.Fatal(err)
}

// Open GORM with the Pinot dialector
db, err := gorm.Open(gormpinot.Open(gormpinot.Config{
    Conn:         conn,
    DefaultTable: "baseballStats",
}), &gorm.Config{})
if err != nil {
    log.Fatal(err)
}
```

## Defining Models

Define Go structs with GORM column tags that match your Pinot table columns:

```go
type Player struct {
    PlayerName string `gorm:"column:playerName"`
    TeamID     string `gorm:"column:teamID"`
    YearID     int    `gorm:"column:yearID"`
    HomeRuns   int    `gorm:"column:homeRuns"`
}
```

## Querying

Use standard GORM query methods:

```go
var players []Player
err = db.Table("baseballStats").
    Select("playerName, teamID, yearID, homeRuns").
    Where("teamID = ? AND yearID = ?", "OAK", 2004).
    Order("homeRuns DESC").
    Limit(5).
    Find(&players).Error
if err != nil {
    log.Fatal(err)
}

for _, p := range players {
    fmt.Printf("%s (%s, %d): %d home runs\n", p.PlayerName, p.TeamID, p.YearID, p.HomeRuns)
}
```

## Aggregation Queries

```go
type TeamStats struct {
    TeamID   string  `gorm:"column:teamID"`
    Count    int64   `gorm:"column:cnt"`
    AvgHR    float64 `gorm:"column:avg_hr"`
}

var stats []TeamStats
err = db.Table("baseballStats").
    Select("teamID, COUNT(*) AS cnt, AVG(homeRuns) AS avg_hr").
    Group("teamID").
    Order("cnt DESC").
    Limit(10).
    Find(&stats).Error
```

## Limitations

- **Read-only**: The Pinot GORM dialector is read-only. Write operations (Create, Update, Delete) are not supported.
- **No migrations**: Pinot table schema is managed outside of GORM.
- **SQL subset**: Only Pinot-supported SQL syntax is available.

## Example

See [`examples/gorm-example/main.go`](https://github.com/startreedata/pinot-client-go/blob/master/examples/gorm-example/main.go) for a runnable example.
