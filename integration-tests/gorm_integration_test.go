package main

import (
	"testing"

	"github.com/startreedata/pinot-client-go/gormpinot"
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

type gormPlayer struct {
	PlayerName string `gorm:"column:playerName"`
	TeamID     string `gorm:"column:teamID"`
	YearID     int    `gorm:"column:yearID"`
	HomeRuns   int    `gorm:"column:homeRuns"`
}

// TestGormQueryIntegration validates GORM queries against a live Pinot cluster.
func TestGormQueryIntegration(t *testing.T) {
	pinotClient := getPinotClientFromBroker(false)
	db := openGormPinot(t, pinotClient)

	var players []gormPlayer
	err := db.Table("baseballStats").
		Select("playerName, teamID, yearID, homeRuns").
		Where("teamID = ? AND yearID = ?", "OAK", 2004).
		Order("homeRuns DESC").
		Limit(5).
		Find(&players).Error

	assert.NoError(t, err)
	assert.NotEmpty(t, players)
	assert.Equal(t, "OAK", players[0].TeamID)
	assert.Equal(t, 2004, players[0].YearID)
}

func openGormPinot(t *testing.T, conn *pinot.Connection) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(gormpinot.Open(gormpinot.Config{
		Conn:         conn,
		DefaultTable: "baseballStats",
	}), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open gorm pinot connection: %v", err)
	}
	return db
}
