package main

import (
	"fmt"

	"github.com/startreedata/pinot-client-go/gormpinot"
	"github.com/startreedata/pinot-client-go/pinot"
	"gorm.io/gorm"
)

type Player struct {
	PlayerName string `gorm:"column:playerName"`
	TeamID     string `gorm:"column:teamID"`
	YearID     int    `gorm:"column:yearID"`
	HomeRuns   int    `gorm:"column:homeRuns"`
}

func main() {
	conn, err := pinot.NewFromBrokerList([]string{"localhost:8000"})
	if err != nil {
		panic(err)
	}

	db, err := gorm.Open(gormpinot.Open(gormpinot.Config{
		Conn:         conn,
		DefaultTable: "baseballStats",
	}), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	var players []Player
	err = db.Table("baseballStats").
		Select("playerName, teamID, yearID, homeRuns").
		Where("teamID = ? AND yearID = ?", "OAK", 2004).
		Order("homeRuns DESC").
		Limit(5).
		Find(&players).Error
	if err != nil {
		panic(err)
	}

	for _, player := range players {
		fmt.Printf("%s (%s) %d HR in %d\n", player.PlayerName, player.TeamID, player.HomeRuns, player.YearID)
	}
}
