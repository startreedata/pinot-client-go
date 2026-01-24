package pinot

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConnection_Prepare(t *testing.T) {
	connection := &Connection{}

	tests := []struct {
		name        string
		table       string
		query       string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid query with single parameter",
			table:       "testTable",
			query:       "SELECT * FROM testTable WHERE id = ?",
			expectError: false,
		},
		{
			name:        "Valid query with multiple parameters",
			table:       "testTable",
			query:       "SELECT * FROM testTable WHERE id = ? AND name = ? AND age > ?",
			expectError: false,
		},
		{
			name:        "Empty table name",
			table:       "",
			query:       "SELECT * FROM testTable WHERE id = ?",
			expectError: true,
			errorMsg:    "table name cannot be empty",
		},
		{
			name:        "Empty query",
			table:       "testTable",
			query:       "",
			expectError: true,
			errorMsg:    "query template cannot be empty",
		},
		{
			name:        "Query without parameters",
			table:       "testTable",
			query:       "SELECT * FROM testTable",
			expectError: true,
			errorMsg:    "query template must contain at least one parameter placeholder (?)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := connection.Prepare(test.table, test.query)

			if test.expectError {
				assert.Error(t, err)
				assert.Nil(t, stmt)
				if test.errorMsg != "" {
					assert.Contains(t, err.Error(), test.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, stmt)
				assert.Equal(t, test.query, stmt.GetQuery())
				assert.Equal(t, strings.Count(test.query, "?"), stmt.GetParameterCount())
			}
		})
	}
}

func TestPreparedStatement_SetParameters(t *testing.T) {
	connection := &Connection{}
	stmt, err := connection.Prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ?")
	assert.NoError(t, err)
	assert.NotNil(t, stmt)

	// Test setting valid parameters
	err = stmt.SetInt(1, 123)
	assert.NoError(t, err)

	err = stmt.SetString(2, "testName")
	assert.NoError(t, err)

	// Test setting parameter with invalid index
	err = stmt.SetInt(0, 123) // Index too low
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parameter index 0 is out of range")

	err = stmt.SetInt(3, 123) // Index too high
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parameter index 3 is out of range")

	// Test different parameter types
	err = stmt.SetInt64(1, int64(456))
	assert.NoError(t, err)

	err = stmt.SetFloat64(1, 3.14)
	assert.NoError(t, err)

	err = stmt.SetBool(1, true)
	assert.NoError(t, err)

	// Test generic Set method
	err = stmt.Set(1, "genericValue")
	assert.NoError(t, err)

	err = stmt.Set(1, time.Now())
	assert.NoError(t, err)
}

func TestPreparedStatement_Execute_WithMockServer(t *testing.T) {
	// Mock HTTP server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		assert.Equal(t, "POST", r.Method)
		assert.True(t, strings.HasSuffix(r.RequestURI, "/query/sql"))
		_, err := fmt.Fprintln(w, `{"resultTable":{"dataSchema":{"columnDataTypes":["LONG","STRING"],"columnNames":["id","name"]},"rows":[[123,"testName"]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":5}`)
		assert.Nil(t, err)
	}))
	defer ts.Close()

	// Create connection
	pinotClient, err := NewFromBrokerList([]string{ts.URL})
	assert.NoError(t, err)
	assert.NotNil(t, pinotClient)

	// Create prepared statement
	stmt, err := pinotClient.Prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ?")
	assert.NoError(t, err)
	assert.NotNil(t, stmt)

	// Test execute without setting all parameters
	_, err = stmt.Execute()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parameter at index 1 is not set")

	// Set parameters and execute
	err = stmt.SetInt(1, 123)
	assert.NoError(t, err)
	err = stmt.SetString(2, "testName")
	assert.NoError(t, err)

	resp, err := stmt.Execute()
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.ResultTable)
}

func TestPreparedStatement_ExecuteWithParams_WithMockServer(t *testing.T) {
	// Mock HTTP server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := fmt.Fprintln(w, `{"resultTable":{"dataSchema":{"columnDataTypes":["LONG","STRING","LONG"],"columnNames":["id","name","age"]},"rows":[[123,"testName",25]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":5}`)
		assert.Nil(t, err)
	}))
	defer ts.Close()

	// Create connection
	pinotClient, err := NewFromBrokerList([]string{ts.URL})
	assert.NoError(t, err)

	// Create prepared statement
	stmt, err := pinotClient.Prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ? AND age > ?")
	assert.NoError(t, err)

	// Test ExecuteWithParams with correct number of parameters
	resp, err := stmt.ExecuteWithParams(123, "testName", 18)
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	// Test ExecuteWithParams with incorrect number of parameters
	_, err = stmt.ExecuteWithParams(123) // Too few parameters
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected 3 parameters, got 1")

	_, err = stmt.ExecuteWithParams(123, "testName", 18, "extra") // Too many parameters
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected 3 parameters, got 4")
}

func TestPreparedStatement_ParameterFormatting(t *testing.T) {
	connection := &Connection{}
	stmt, err := connection.Prepare("testTable", "SELECT * FROM testTable WHERE col1 = ? AND col2 = ? AND col3 = ? AND col4 = ? AND col5 = ?")
	assert.NoError(t, err)

	ps, ok := stmt.(*preparedStatement)
	assert.True(t, ok, "Expected stmt to be a *preparedStatement")

	// Test parameter formatting
	params := []interface{}{
		"string_value",
		123,
		3.14,
		true,
		time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	query, err := ps.buildQuery(params)
	assert.NoError(t, err)

	expected := "SELECT * FROM testTable WHERE col1 = 'string_value' AND col2 = 123 AND col3 = 3.14 AND col4 = true AND col5 = '2023-01-01 12:00:00.000'"
	assert.Equal(t, expected, query)
}

func TestPreparedStatement_ClearParameters(t *testing.T) {
	connection := &Connection{}
	stmt, err := connection.Prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ?")
	assert.NoError(t, err)

	// Set parameters
	err = stmt.SetInt(1, 123)
	assert.NoError(t, err)
	err = stmt.SetString(2, "testName")
	assert.NoError(t, err)

	// Clear parameters
	err = stmt.ClearParameters()
	assert.NoError(t, err)

	// Try to execute - should fail because parameters are cleared
	_, err = stmt.Execute()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parameter at index 1 is not set")
}

func TestPreparedStatement_Close(t *testing.T) {
	connection := &Connection{}
	stmt, err := connection.Prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
	assert.NoError(t, err)

	// Close the statement
	err = stmt.Close()
	assert.NoError(t, err)

	// Try to use the closed statement
	err = stmt.SetInt(1, 123)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "prepared statement is closed")

	_, err = stmt.Execute()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "prepared statement is closed")

	_, err = stmt.ExecuteWithParams(123)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "prepared statement is closed")

	err = stmt.ClearParameters()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "prepared statement is closed")
}

func TestPreparedStatement_GetMethods(t *testing.T) {
	connection := &Connection{}
	queryTemplate := "SELECT * FROM testTable WHERE id = ? AND name = ? AND age > ?"
	stmt, err := connection.Prepare("testTable", queryTemplate)
	assert.NoError(t, err)

	// Test GetQuery
	assert.Equal(t, queryTemplate, stmt.GetQuery())

	// Test GetParameterCount
	assert.Equal(t, 3, stmt.GetParameterCount())
}

func TestPreparedStatement_ComplexQueryFormattingLikeJava(t *testing.T) {
	connection := &Connection{}

	// Complex query similar to what might be used in Java PreparedStatement examples
	stmt, err := connection.Prepare("baseballStats",
		"SELECT playerName, sum(homeRuns) as totalHomeRuns "+
			"FROM baseballStats "+
			"WHERE homeRuns > ? AND teamID = ? AND yearID BETWEEN ? AND ? "+
			"GROUP BY playerID, playerName "+
			"ORDER BY totalHomeRuns DESC "+
			"LIMIT ?")
	assert.NoError(t, err)
	assert.Equal(t, 5, stmt.GetParameterCount())

	ps, ok := stmt.(*preparedStatement)
	assert.True(t, ok, "Expected stmt to be a *preparedStatement")

	// Test with typical parameters
	params := []interface{}{0, "OAK", 2000, 2010, 10}
	query, err := ps.buildQuery(params)
	assert.NoError(t, err)

	expected := "SELECT playerName, sum(homeRuns) as totalHomeRuns " +
		"FROM baseballStats " +
		"WHERE homeRuns > 0 AND teamID = 'OAK' AND yearID BETWEEN 2000 AND 2010 " +
		"GROUP BY playerID, playerName " +
		"ORDER BY totalHomeRuns DESC " +
		"LIMIT 10"
	assert.Equal(t, expected, query)
}

func TestPreparedStatement_ConcurrentUsage(t *testing.T) {
	connection := &Connection{}
	stmt, err := connection.Prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
	assert.NoError(t, err)

	// Test concurrent parameter setting (should be thread-safe)
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()
			setErr := stmt.SetInt(1, id)
			assert.NoError(t, setErr)
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	err = stmt.Close()
	assert.NoError(t, err)
}
