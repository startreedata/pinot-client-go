package main

import (
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
