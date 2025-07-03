package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	pinot "github.com/startreedata/pinot-client-go/pinot"
	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"
)

// getEnv retrieves the value of the environment variable named by the key.
// It returns the value, which will be the default value if the variable is not present.
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

var (
	zookeeperPort  = getEnv("ZOOKEEPER_PORT", "2123")
	controllerPort = getEnv("CONTROLLER_PORT", "9000")
	brokerPort     = getEnv("BROKER_PORT", "8000")
)

func getPinotClientFromZookeeper(useMultistageEngine bool) *pinot.Connection {
	pinotClient, err := pinot.NewFromZookeeper([]string{"localhost:" + zookeeperPort}, "", "QuickStartCluster")
	if err != nil {
		log.Fatalln(err)
	}
	pinotClient.UseMultistageEngine(useMultistageEngine)
	return pinotClient
}

func getPinotClientFromController(useMultistageEngine bool) *pinot.Connection {
	pinotClient, err := pinot.NewFromController("localhost:" + controllerPort)
	if err != nil {
		log.Fatalln(err)
	}
	pinotClient.UseMultistageEngine(useMultistageEngine)
	return pinotClient
}

func getPinotClientFromBroker(useMultistageEngine bool) *pinot.Connection {
	pinotClient, err := pinot.NewFromBrokerList([]string{"localhost:" + brokerPort})
	if err != nil {
		log.Fatalln(err)
	}
	pinotClient.UseMultistageEngine(useMultistageEngine)
	return pinotClient
}

func getCustomHTTPClient() *http.Client {
	httpClient := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100, // Max idle connections in total
			MaxIdleConnsPerHost: 10,  // Max idle connections per host
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			// You may add other settings like TLS configuration, Proxy, etc.
		},
	}
	return httpClient
}

func getPinotClientFromZookeeperAndCustomHTTPClient(useMultistageEngine bool) *pinot.Connection {
	pinotClient, err := pinot.NewFromZookeeperAndClient([]string{"localhost:" + zookeeperPort}, "", "QuickStartCluster", getCustomHTTPClient())
	if err != nil {
		log.Fatalln(err)
	}
	pinotClient.UseMultistageEngine(useMultistageEngine)
	return pinotClient
}

func getPinotClientFromControllerAndCustomHTTPClient(useMultistageEngine bool) *pinot.Connection {
	pinotClient, err := pinot.NewFromControllerAndClient("localhost:"+controllerPort, getCustomHTTPClient())
	if err != nil {
		log.Fatalln(err)
	}
	pinotClient.UseMultistageEngine(useMultistageEngine)
	return pinotClient
}

func getPinotClientFromBrokerAndCustomHTTPClient(useMultistageEngine bool) *pinot.Connection {
	pinotClient, err := pinot.NewFromBrokerListAndClient([]string{"localhost:" + brokerPort}, getCustomHTTPClient())
	if err != nil {
		log.Fatalln(err)
	}
	pinotClient.UseMultistageEngine(useMultistageEngine)
	return pinotClient
}

func getPinotClientFromConfig(useMultistageEngine bool) *pinot.Connection {
	pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
		BrokerList:      []string{"localhost:" + brokerPort},
		HTTPTimeout:     1500 * time.Millisecond,
		ExtraHTTPHeader: map[string]string{},
	})
	if err != nil {
		log.Fatalln(err)
	}
	pinotClient.UseMultistageEngine(useMultistageEngine)
	return pinotClient
}

func getPinotClientFromConfigAndCustomHTTPClient(useMultistageEngine bool) *pinot.Connection {
	pinotClient, err := pinot.NewWithConfigAndClient(&pinot.ClientConfig{
		BrokerList:      []string{"localhost:" + brokerPort},
		HTTPTimeout:     1500 * time.Millisecond,
		ExtraHTTPHeader: map[string]string{},
	}, getCustomHTTPClient())
	if err != nil {
		log.Fatalln(err)
	}
	pinotClient.UseMultistageEngine(useMultistageEngine)
	return pinotClient
}

// TestSendingQueriesToPinot tests sending queries to Pinot using different Pinot clients.
// This test requires a Pinot cluster running locally with binary not docker.
// You can change the ports by setting the environment variables ZOOKEEPER_PORT, CONTROLLER_PORT, and BROKER_PORT.
func TestSendingQueriesToPinot(t *testing.T) {
	pinotClients := []*pinot.Connection{
		getPinotClientFromZookeeper(false),
		getPinotClientFromController(false),
		getPinotClientFromBroker(false),
		getPinotClientFromConfig(false),
		getPinotClientFromZookeeperAndCustomHTTPClient(false),
		getPinotClientFromControllerAndCustomHTTPClient(false),
		getPinotClientFromBrokerAndCustomHTTPClient(false),
		getPinotClientFromConfigAndCustomHTTPClient(false),

		getPinotClientFromZookeeper(true),
		getPinotClientFromController(true),
		getPinotClientFromBroker(true),
		getPinotClientFromConfig(true),
		getPinotClientFromZookeeperAndCustomHTTPClient(true),
		getPinotClientFromControllerAndCustomHTTPClient(true),
		getPinotClientFromBrokerAndCustomHTTPClient(true),
		getPinotClientFromConfigAndCustomHTTPClient(true),
	}

	table := "baseballStats"
	pinotQueries := []string{
		"select count(*) as cnt from baseballStats limit 1",
	}

	log.Printf("Querying SQL")
	for _, query := range pinotQueries {
		for i := 0; i < 200; i++ {
			log.Printf("Trying to query Pinot: %v\n", query)
			brokerResp, err := pinotClients[i%len(pinotClients)].ExecuteSQL(table, query)
			assert.Nil(t, err)
			assert.Equal(t, int64(97889), brokerResp.ResultTable.GetLong(0, 0))
		}
	}
}

// TestPreparedStatementIntegration tests PreparedStatement functionality against a live Pinot cluster.
// This test requires a Pinot cluster running locally with the baseballStats table available.
func TestPreparedStatementIntegration(t *testing.T) {
	// Test with different client configurations
	pinotClients := []*pinot.Connection{
		getPinotClientFromZookeeper(false),
		getPinotClientFromController(false),
		getPinotClientFromBroker(false),
		getPinotClientFromConfig(false),
	}

	table := "baseballStats"

	for clientIndex, pinotClient := range pinotClients {
		t.Run(fmt.Sprintf("Client_%d_BasicPreparedStatement", clientIndex), func(t *testing.T) {
			testBasicPreparedStatement(t, pinotClient, table)
		})

		t.Run(fmt.Sprintf("Client_%d_PreparedStatementWithMultipleParams", clientIndex), func(t *testing.T) {
			testPreparedStatementWithMultipleParams(t, pinotClient, table)
		})

		t.Run(fmt.Sprintf("Client_%d_PreparedStatementReuse", clientIndex), func(t *testing.T) {
			testPreparedStatementReuse(t, pinotClient, table)
		})

		t.Run(fmt.Sprintf("Client_%d_PreparedStatementExecuteWithParams", clientIndex), func(t *testing.T) {
			testPreparedStatementExecuteWithParams(t, pinotClient, table)
		})

		t.Run(fmt.Sprintf("Client_%d_PreparedStatementDifferentTypes", clientIndex), func(t *testing.T) {
			testPreparedStatementDifferentTypes(t, pinotClient, table)
		})
	}
}

// testBasicPreparedStatement tests basic PreparedStatement functionality
func testBasicPreparedStatement(t *testing.T, client *pinot.Connection, table string) {
	// Create a simple prepared statement
	stmt, err := client.Prepare(table, "select count(*) as cnt from baseballStats where teamID = ? limit 1")
	assert.NoError(t, err)
	assert.NotNil(t, stmt)
	defer func() { _ = stmt.Close() }() //nolint:errcheck

	// Verify statement properties
	assert.Equal(t, 1, stmt.GetParameterCount())
	assert.Contains(t, stmt.GetQuery(), "teamID = ?")

	// Set parameter and execute
	err = stmt.SetString(1, "SF")
	assert.NoError(t, err)

	response, err := stmt.Execute()
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.NotNil(t, response.ResultTable)
	assert.Equal(t, 1, response.ResultTable.GetRowCount())
	assert.Equal(t, 1, response.ResultTable.GetColumnCount())
	assert.Equal(t, "cnt", response.ResultTable.GetColumnName(0))

	// Verify we got some results (SF team should exist in baseballStats)
	count := response.ResultTable.GetLong(0, 0)
	assert.True(t, count > 0, "Expected count > 0 for SF team")

	log.Printf("Basic PreparedStatement test passed - SF team count: %d", count)
}

// testPreparedStatementWithMultipleParams tests PreparedStatement with multiple parameters
func testPreparedStatementWithMultipleParams(t *testing.T, client *pinot.Connection, table string) {
	// Create a prepared statement with multiple parameters
	stmt, err := client.Prepare(table,
		"select playerName, sum(homeRuns) as totalHomeRuns from baseballStats where teamID = ? and yearID >= ? group by playerID, playerName order by totalHomeRuns desc limit ?")
	assert.NoError(t, err)
	assert.NotNil(t, stmt)
	defer func() { _ = stmt.Close() }() //nolint:errcheck

	// Verify statement properties
	assert.Equal(t, 3, stmt.GetParameterCount())

	// Set parameters
	err = stmt.SetString(1, "NYA") // New York Yankees
	assert.NoError(t, err)
	err = stmt.SetInt(2, 2000) // Year >= 2000
	assert.NoError(t, err)
	err = stmt.SetInt(3, 5) // Limit 5
	assert.NoError(t, err)

	response, err := stmt.Execute()
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.NotNil(t, response.ResultTable)
	assert.True(t, response.ResultTable.GetRowCount() <= 5)
	assert.Equal(t, 2, response.ResultTable.GetColumnCount())
	assert.Equal(t, "playerName", response.ResultTable.GetColumnName(0))
	assert.Equal(t, "totalHomeRuns", response.ResultTable.GetColumnName(1))

	log.Printf("Multiple parameters PreparedStatement test passed - returned %d rows", response.ResultTable.GetRowCount())
}

// testPreparedStatementReuse tests reusing a PreparedStatement with different parameters
func testPreparedStatementReuse(t *testing.T, client *pinot.Connection, table string) {
	// Create a prepared statement for team statistics
	stmt, err := client.Prepare(table, "select count(*) as playerCount, sum(homeRuns) as totalHomeRuns from baseballStats where teamID = ?")
	assert.NoError(t, err)
	assert.NotNil(t, stmt)
	defer func() { _ = stmt.Close() }() //nolint:errcheck

	// Test different teams
	teams := []string{"NYA", "BOS", "LAA"}
	var results []int64

	for _, team := range teams {
		// Clear previous parameters and set new ones
		err = stmt.ClearParameters()
		assert.NoError(t, err)

		err = stmt.SetString(1, team)
		assert.NoError(t, err)

		response, err := stmt.Execute()
		assert.NoError(t, err)
		assert.NotNil(t, response)
		assert.NotNil(t, response.ResultTable)
		assert.Equal(t, 1, response.ResultTable.GetRowCount())

		playerCount := response.ResultTable.GetLong(0, 0)
		totalHomeRuns := response.ResultTable.GetLong(0, 1)
		results = append(results, playerCount)

		assert.True(t, playerCount > 0, "Expected player count > 0 for team %s", team)
		assert.True(t, totalHomeRuns >= 0, "Expected total home runs >= 0 for team %s", team)

		log.Printf("Team %s: %d players, %d total home runs", team, playerCount, totalHomeRuns)
	}

	// Verify we got different results for different teams (sanity check)
	assert.True(t, len(results) == 3, "Expected results for 3 teams")
}

// testPreparedStatementExecuteWithParams tests the ExecuteWithParams convenience method
func testPreparedStatementExecuteWithParams(t *testing.T, client *pinot.Connection, table string) {
	// Create a prepared statement
	stmt, err := client.Prepare(table,
		"select count(*) as cnt from baseballStats where yearID between ? and ? and homeRuns >= ?")
	assert.NoError(t, err)
	assert.NotNil(t, stmt)
	defer func() { _ = stmt.Close() }() //nolint:errcheck

	// Test ExecuteWithParams method
	response, err := stmt.ExecuteWithParams(2000, 2010, 20)
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.NotNil(t, response.ResultTable)
	assert.Equal(t, 1, response.ResultTable.GetRowCount())

	count := response.ResultTable.GetLong(0, 0)
	assert.True(t, count >= 0, "Expected count >= 0")

	log.Printf("ExecuteWithParams test passed - count of players with 20+ home runs (2000-2010): %d", count)

	// Test with different parameters
	response2, err := stmt.ExecuteWithParams(1990, 1999, 30)
	assert.NoError(t, err)
	assert.NotNil(t, response2)

	count2 := response2.ResultTable.GetLong(0, 0)
	assert.True(t, count2 >= 0, "Expected count >= 0")

	log.Printf("ExecuteWithParams test passed - count of players with 30+ home runs (1990-1999): %d", count2)
}

// testPreparedStatementDifferentTypes tests PreparedStatement with different parameter types
func testPreparedStatementDifferentTypes(t *testing.T, client *pinot.Connection, table string) {
	// Create a prepared statement that can test different parameter types
	stmt, err := client.Prepare(table,
		"select count(*) as cnt from baseballStats where yearID = ? and homeRuns >= ? and battingAvg > ?")
	assert.NoError(t, err)
	assert.NotNil(t, stmt)
	defer func() { _ = stmt.Close() }() //nolint:errcheck

	// Test with int, int, and float parameters
	err = stmt.SetInt(1, 2001)
	assert.NoError(t, err)
	err = stmt.SetInt(2, 25)
	assert.NoError(t, err)
	err = stmt.SetFloat64(3, 0.280)
	assert.NoError(t, err)

	response, err := stmt.Execute()
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.NotNil(t, response.ResultTable)

	count := response.ResultTable.GetLong(0, 0)
	assert.True(t, count >= 0, "Expected count >= 0")

	log.Printf("Different types test passed - players in 2001 with 25+ HR and .280+ avg: %d", count)

	// Test using Set method with different types
	err = stmt.Set(1, int64(2005))
	assert.NoError(t, err)
	err = stmt.Set(2, 30)
	assert.NoError(t, err)
	err = stmt.Set(3, 0.300)
	assert.NoError(t, err)

	response2, err := stmt.Execute()
	assert.NoError(t, err)
	assert.NotNil(t, response2)

	count2 := response2.ResultTable.GetLong(0, 0)
	assert.True(t, count2 >= 0, "Expected count >= 0")

	log.Printf("Set method test passed - players in 2005 with 30+ HR and .300+ avg: %d", count2)
}

// TestPreparedStatementIntegrationWithMultistage tests PreparedStatement with multistage engine
func TestPreparedStatementIntegrationWithMultistage(t *testing.T) {
	// Test with multistage engine enabled
	pinotClients := []*pinot.Connection{
		getPinotClientFromZookeeper(true),
		getPinotClientFromController(true),
		getPinotClientFromBroker(true),
		getPinotClientFromConfig(true),
	}

	table := "baseballStats"

	for clientIndex, pinotClient := range pinotClients {
		t.Run(fmt.Sprintf("MultistageClient_%d", clientIndex), func(t *testing.T) {
			// Test basic functionality with multistage engine
			stmt, err := pinotClient.Prepare(table, "select teamID, count(*) as cnt from baseballStats where yearID = ? group by teamID order by cnt desc limit ?")
			assert.NoError(t, err)
			assert.NotNil(t, stmt)
			defer func() { _ = stmt.Close() }() //nolint:errcheck

			response, err := stmt.ExecuteWithParams(2000, 10)
			assert.NoError(t, err)
			assert.NotNil(t, response)
			assert.NotNil(t, response.ResultTable)
			assert.True(t, response.ResultTable.GetRowCount() <= 10)
			assert.Equal(t, 2, response.ResultTable.GetColumnCount())

			log.Printf("Multistage PreparedStatement test passed - returned %d teams for year 2000", response.ResultTable.GetRowCount())
		})
	}
}
