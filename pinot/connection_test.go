package pinot

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		err := json.NewDecoder(r.Body).Decode(&request)
		assert.Equal(t, request["trace"], "true")
		assert.Nil(t, err)
	}))
	defer ts.Close()
	pinotClient, err := NewFromBrokerList([]string{ts.URL})
	assert.NotNil(t, pinotClient)
	assert.NotNil(t, pinotClient.brokerSelector)
	assert.NotNil(t, pinotClient.transport)
	assert.Nil(t, err)
	pinotClient.OpenTrace()
	resp, err := pinotClient.ExecuteSQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
}

func TestSendingQueryWithTraceClose(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]string
		err := json.NewDecoder(r.Body).Decode(&request)
		assert.Nil(t, err)
		_, ok := request["trace"]
		assert.False(t, ok)
	}))
	defer ts.Close()
	pinotClient, err := NewFromBrokerList([]string{ts.URL})
	assert.NotNil(t, pinotClient)
	assert.NotNil(t, pinotClient.brokerSelector)
	assert.NotNil(t, pinotClient.transport)
	assert.Nil(t, err)
	resp, err := pinotClient.ExecuteSQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	pinotClient.OpenTrace()
	pinotClient.CloseTrace()
	resp, err = pinotClient.ExecuteSQL("", "select teamID, count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
}

func TestFormatQuery(t *testing.T) {
	// Test case 1: No parameters
	queryPattern := "SELECT * FROM table"
	expectedQuery := "SELECT * FROM table"
	actualQuery, err := formatQuery(queryPattern, nil)
	assert.Nil(t, err)
	assert.Equal(t, expectedQuery, actualQuery)

	// Test case 2: Single parameter
	queryPattern = "SELECT * FROM table WHERE id = ?"
	params := []interface{}{42}
	expectedQuery = "SELECT * FROM table WHERE id = 42"
	actualQuery, err = formatQuery(queryPattern, params)
	assert.Nil(t, err)
	assert.Equal(t, expectedQuery, actualQuery)

	// Test case 3: Multiple parameters
	queryPattern = "SELECT * FROM table WHERE id = ? AND name = ?"
	params = []interface{}{42, "John"}
	expectedQuery = "SELECT * FROM table WHERE id = 42 AND name = 'John'"
	actualQuery, err = formatQuery(queryPattern, params)
	assert.Nil(t, err)
	assert.Equal(t, expectedQuery, actualQuery)

	// Test case 4: Invalid query pattern
	queryPattern = "SELECT * FROM table WHERE id = ? AND name = ?"
	params = []interface{}{42} // Missing second parameter
	expectedQuery = ""         // Empty query
	actualQuery, err = formatQuery(queryPattern, params)
	assert.NotNil(t, err)
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestFormatArg(t *testing.T) {
	// Test case 1: string value
	value1 := "hello"
	expected1 := "'hello'"
	actual1, err := formatArg(value1)
	assert.Nil(t, err)
	assert.Equal(t, expected1, actual1)

	// Test case 2: time.Time value
	value2 := time.Date(2022, time.January, 1, 12, 0, 0, 0, time.UTC)
	expected2 := "'2022-01-01 12:00:00.000'"
	actual2, err := formatArg(value2)
	assert.Nil(t, err)
	assert.Equal(t, expected2, actual2)

	// Test case 3: int value
	value3 := 42
	expected3 := "42"
	actual3, err := formatArg(value3)
	assert.Nil(t, err)
	assert.Equal(t, expected3, actual3)

	// Test case 4: big.Int value
	value4 := big.NewInt(1234567890)
	expected4 := "'1234567890'"
	actual4, err := formatArg(value4)
	assert.Nil(t, err)
	assert.Equal(t, expected4, actual4)

	// Test case 5: float32 value
	value5 := float32(3.14)
	expected5 := "3.14"
	actual5, err := formatArg(value5)
	assert.Nil(t, err)
	assert.Equal(t, expected5, actual5)

	// Test case 6: float64 value
	value6 := float64(3.14159)
	expected6 := "3.14159"
	actual6, err := formatArg(value6)
	assert.Nil(t, err)
	assert.Equal(t, expected6, actual6)

	// Test case 7: bool value
	value7 := true
	expected7 := "true"
	actual7, err := formatArg(value7)
	assert.Nil(t, err)
	assert.Equal(t, expected7, actual7)

	// Test case 8: unsupported type
	value8 := struct{}{}
	expected8 := "unsupported type: struct {}"
	_, err = formatArg(value8)
	assert.NotNil(t, err)
	assert.Equal(t, expected8, err.Error())

	// Test case 9: big.Float value
	value9 := big.NewFloat(3.141592653589793238)
	expected9 := "'3.141592653589793'"
	actual9, err := formatArg(value9)
	assert.Nil(t, err)
	assert.Equal(t, expected9, actual9)

	// Test case 10: byte array value
	value10 := []byte{0x48, 0x65, 0x6c, 0x6c, 0x6f}
	expected10 := "'48656c6c6f'"
	actual10, err := formatArg(value10)
	assert.Nil(t, err)
	assert.Equal(t, expected10, actual10)
}

type mockBrokerSelector struct {
	mock.Mock
}

func (m *mockBrokerSelector) init() error { return nil }
func (m *mockBrokerSelector) selectBroker(table string) (string, error) {
	args := m.Called(table)
	return args.Get(0).(string), args.Error(1)
}

type mockTransport struct {
	mock.Mock
}

func (m *mockTransport) execute(brokerAddress string, query *Request) (*BrokerResponse, error) {
	args := m.Called(brokerAddress, query)
	return args.Get(0).(*BrokerResponse), args.Error(1)
}

func TestExecuteSQLWithParams(t *testing.T) {
	mockBrokerSelector := &mockBrokerSelector{}
	mockTransport := &mockTransport{}

	// Create Connection with mock brokerSelector and transport
	conn := &Connection{
		brokerSelector: mockBrokerSelector,
		transport:      mockTransport,
	}

	// Test case 1: Successful execution
	mockBrokerSelector.On("selectBroker", "baseballStats").Return("host1:8000", nil)
	mockTransport.On("execute", "host1:8000", mock.Anything).Return(&BrokerResponse{}, nil)

	queryPattern := "SELECT * FROM table WHERE id = ?"
	params := []interface{}{42}
	expectedQuery := "SELECT * FROM table WHERE id = 42"
	expectedBrokerResp := &BrokerResponse{}
	mockTransport.On("execute", "host1:8000", &Request{
		queryFormat:         "sql",
		query:               expectedQuery,
		trace:               false,
		useMultistageEngine: false,
	}).Return(expectedBrokerResp, nil)

	brokerResp, err := conn.ExecuteSQLWithParams("baseballStats", queryPattern, params)

	assert.Nil(t, err)
	assert.Equal(t, expectedBrokerResp, brokerResp)

	// Test case 2: Error in selecting broker
	mockBrokerSelector.On("selectBroker", "baseballStats2").Return("", fmt.Errorf("error selecting broker"))

	_, err = conn.ExecuteSQLWithParams("baseballStats2", queryPattern, params)

	assert.NotNil(t, err)
	assert.EqualError(t, err, "error selecting broker")

	// Test case 3: Error in formatting query
	mockBrokerSelector.On("selectBroker", "baseballStats3").Return("host2:8000", nil)
	mockTransport.On("execute", "host2:8000", mock.Anything).Return(&BrokerResponse{}, fmt.Errorf("error executing query"))

	_, err = conn.ExecuteSQLWithParams("baseballStats3", queryPattern, params)

	assert.NotNil(t, err)
	assert.EqualError(t, err, "error executing query")

	// Test case 4: Error in formatting query with mismatched number of parameters
	queryPattern = "SELECT * FROM table WHERE id = ? AND name = ?"
	params = []interface{}{42} // Missing second parameter
	_, err = conn.ExecuteSQLWithParams("baseballStats", queryPattern, params)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "failed to format query: number of placeholders in queryPattern (2) does not match number of params (1)")

	// Test case 5: Unsupported argument type
	queryPattern = "SELECT * FROM table WHERE id = ?"
	params = []interface{}{struct{}{}}
	_, err = conn.ExecuteSQLWithParams("baseballStats", queryPattern, params)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "failed to format query: failed to format parameter: unsupported type: struct {}")
}
