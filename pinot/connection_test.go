package pinot

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSendingSQLWithMockServer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		assert.Equal(t, "POST", r.Method)
		assert.True(t, strings.HasSuffix(r.RequestURI, "/query/sql"))
		fmt.Fprintln(w, "{\"resultTable\":{\"dataSchema\":{\"columnDataTypes\":[\"LONG\"],\"columnNames\":[\"cnt\"]},\"rows\":[[97889]]},\"exceptions\":[],\"numServersQueried\":1,\"numServersResponded\":1,\"numSegmentsQueried\":1,\"numSegmentsProcessed\":1,\"numSegmentsMatched\":1,\"numConsumingSegmentsQueried\":0,\"numDocsScanned\":97889,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":0,\"numGroupsLimitReached\":false,\"totalDocs\":97889,\"timeUsedMs\":5,\"segmentStatistics\":[],\"traceInfo\":{},\"minConsumingFreshnessTimeMs\":0}")
	}))
	defer ts.Close()
	pinotClient, err := NewFromBrokerList([]string{ts.URL})
	assert.NotNil(t, pinotClient)
	assert.NotNil(t, pinotClient.brokerSelector)
	assert.NotNil(t, pinotClient.transport)
	assert.Nil(t, err)
	resp, err := pinotClient.ExecuteSQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
	assert.NotNil(t, resp)
	assert.Nil(t, err)

	// Examine ResultTable
	assert.Equal(t, 1, resp.ResultTable.GetRowCount())
	assert.Equal(t, 1, resp.ResultTable.GetColumnCount())
	assert.Equal(t, "cnt", resp.ResultTable.GetColumnName(0))
	assert.Equal(t, "LONG", resp.ResultTable.GetColumnDataType(0))
	assert.Equal(t, json.Number("97889"), resp.ResultTable.Get(0, 0))
	assert.Equal(t, int32(97889), resp.ResultTable.GetInt(0, 0))
	assert.Equal(t, int64(97889), resp.ResultTable.GetLong(0, 0))
	assert.Equal(t, float32(97889), resp.ResultTable.GetFloat(0, 0))
	assert.Equal(t, float64(97889), resp.ResultTable.GetDouble(0, 0))

	badPinotClient := &Connection{
		transport: &jsonAsyncHTTPClientTransport{
			client: http.DefaultClient,
		},
		brokerSelector: &simpleBrokerSelector{
			brokerList: []string{},
		},
	}
	_, err = badPinotClient.ExecuteSQL("", "")
	assert.NotNil(t, err)
}

func TestSendingQueryWithErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()
	pinotClient, err := NewFromBrokerList([]string{ts.URL})
	assert.NotNil(t, pinotClient)
	assert.NotNil(t, pinotClient.brokerSelector)
	assert.NotNil(t, pinotClient.transport)
	assert.Nil(t, err)
	_, err = pinotClient.ExecuteSQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
	assert.NotNil(t, err)
}

func TestSendingQueryWithNonJsonResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, `ProcessingException`)
	}))
	defer ts.Close()
	pinotClient, err := NewFromBrokerList([]string{ts.URL})
	assert.NotNil(t, pinotClient)
	assert.NotNil(t, pinotClient.brokerSelector)
	assert.NotNil(t, pinotClient.transport)
	assert.Nil(t, err)
	_, err = pinotClient.ExecuteSQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "invalid character"))
}

func TestConnectionWithControllerBasedBrokerSelector(t *testing.T) {
	firstRequest := true
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		assert.Equal(t, "GET", r.Method)
		assert.True(t, strings.HasSuffix(r.RequestURI, "/v2/brokers/tables?state=ONLINE"))
		if firstRequest {
			firstRequest = false
			fmt.Fprintln(w, `{"baseballStats":[{"port":8000,"host":"host1","instanceName":"Broker_host1_8000"}]}`)
		} else {
			fmt.Fprintln(w, `{"baseballStats":[{"port":8000,"host":"host2","instanceName":"Broker_host2_8000"}]}`)
		}
	}))
	defer ts.Close()
	pinotClient, err := NewFromController(ts.URL)
	assert.Nil(t, err)
	selectedBroker, err := pinotClient.brokerSelector.selectBroker("baseballStats")
	assert.Nil(t, err)
	assert.Equal(t, selectedBroker, "host1:8000")
	time.Sleep(1500 * time.Millisecond)
	selectedBroker, err = pinotClient.brokerSelector.selectBroker("baseballStats")
	assert.Nil(t, err)
	assert.Equal(t, selectedBroker, "host2:8000")
}

func TestSendingQueryWithTraceOpen(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]string
		json.NewDecoder(r.Body).Decode(&request)
		assert.Equal(t, request["trace"], "true")
	}))
	defer ts.Close()
	pinotClient, err := NewFromBrokerList([]string{ts.URL})
	assert.NotNil(t, pinotClient)
	assert.NotNil(t, pinotClient.brokerSelector)
	assert.NotNil(t, pinotClient.transport)
	assert.Nil(t, err)
	pinotClient.OpenTrace()
	pinotClient.ExecuteSQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
}

func TestSendingQueryWithTraceClose(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]string
		json.NewDecoder(r.Body).Decode(&request)
		_, ok := request["trace"]
		assert.False(t, ok)
	}))
	defer ts.Close()
	pinotClient, err := NewFromBrokerList([]string{ts.URL})
	assert.NotNil(t, pinotClient)
	assert.NotNil(t, pinotClient.brokerSelector)
	assert.NotNil(t, pinotClient.transport)
	assert.Nil(t, err)
	pinotClient.ExecuteSQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
	pinotClient.OpenTrace()
	pinotClient.CloseTrace()
	pinotClient.ExecuteSQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
}
