package pinot

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	assert.Equal(t, float64(97889), resp.ResultTable.Get(0, 0))
	assert.Equal(t, 97889, resp.ResultTable.GetInt(0, 0))
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

func TestSendingPQLWithMockServer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		assert.Equal(t, "POST", r.Method)
		assert.True(t, strings.HasSuffix(r.RequestURI, "/query"))
		fmt.Fprintln(w, `{"aggregationResults":[{"groupByResult":[{"value":"4720","group":["CHN"]},{"value":"4621","group":["PHI"]},{"value":"4575","group":["PIT"]},{"value":"4535","group":["SLN"]},{"value":"4393","group":["CIN"]},{"value":"4318","group":["CLE"]},{"value":"4130","group":["BOS"]},{"value":"4111","group":["CHA"]},{"value":"4069","group":["NYA"]},{"value":"4051","group":["DET"]}],"function":"count_star","groupByColumns":["teamID"]},{"groupByResult":[{"value":"14859.00000","group":["NYA"]},{"value":"13202.00000","group":["CHN"]},{"value":"12854.00000","group":["DET"]},{"value":"12599.00000","group":["BOS"]},{"value":"12248.00000","group":["PHI"]},{"value":"12085.00000","group":["CIN"]},{"value":"12050.00000","group":["CLE"]},{"value":"10915.00000","group":["SLN"]},{"value":"10582.00000","group":["PIT"]},{"value":"10501.00000","group":["CHA"]}],"function":"sum_homeRuns","groupByColumns":["teamID"]}],"exceptions":[],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":1,"numSegmentsMatched":1,"numConsumingSegmentsQueried":0,"numDocsScanned":97889,"numEntriesScannedInFilter":0,"numEntriesScannedPostFilter":195778,"numGroupsLimitReached":false,"totalDocs":97889,"timeUsedMs":7,"segmentStatistics":[],"traceInfo":{},"minConsumingFreshnessTimeMs":0}`)
	}))
	defer ts.Close()
	pinotClient, err := NewFromBrokerList([]string{ts.URL})
	assert.NotNil(t, pinotClient)
	assert.NotNil(t, pinotClient.brokerSelector)
	assert.NotNil(t, pinotClient.transport)
	assert.Nil(t, err)
	resp, err := pinotClient.ExecutePQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
	assert.NotNil(t, resp)
	assert.Nil(t, err)

	// Examine ResultTable
	assert.Equal(t, 2, len(resp.AggregationResults))
	assert.Equal(t, 10, len(resp.AggregationResults[0].GroupByResult))
	assert.Equal(t, 1, len(resp.AggregationResults[0].GroupByResult[0].Group))
	assert.Equal(t, "4720", resp.AggregationResults[0].GroupByResult[0].Value)
	assert.Equal(t, "CHN", resp.AggregationResults[0].GroupByResult[0].Group[0])
	assert.Equal(t, "count_star", resp.AggregationResults[0].Function)
	assert.Equal(t, 1, len(resp.AggregationResults[0].GroupByColumns))
	assert.Equal(t, "teamID", resp.AggregationResults[0].GroupByColumns[0])

	assert.Equal(t, 10, len(resp.AggregationResults[1].GroupByResult))
	assert.Equal(t, 1, len(resp.AggregationResults[1].GroupByResult[0].Group))
	assert.Equal(t, "14859.00000", resp.AggregationResults[1].GroupByResult[0].Value)
	assert.Equal(t, "NYA", resp.AggregationResults[1].GroupByResult[0].Group[0])
	assert.Equal(t, "sum_homeRuns", resp.AggregationResults[1].Function)
	assert.Equal(t, 1, len(resp.AggregationResults[1].GroupByColumns))
	assert.Equal(t, "teamID", resp.AggregationResults[1].GroupByColumns[0])

	badPinotClient := &Connection{
		transport: &jsonAsyncHTTPClientTransport{
			client: http.DefaultClient,
		},
		brokerSelector: &simpleBrokerSelector{
			brokerList: []string{},
		},
	}
	_, err = badPinotClient.ExecutePQL("", "")
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
	_, err = pinotClient.ExecutePQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
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
	_, err = pinotClient.ExecutePQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "invalid character"))
}
