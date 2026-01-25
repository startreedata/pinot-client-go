package gormpinot

import (
	"context"
	"database/sql/driver"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/startreedata/pinot-client-go/pinot"
)

func TestPinotDriverOpenRequiresConnector(t *testing.T) {
	_, err := pinotDriver{}.Open("")
	require.Error(t, err)
}

func TestConnectorAndConnBasics(t *testing.T) {
	connector := newConnector(&pinot.Connection{}, "baseballStats")
	driverConn, err := connector.Connect(context.Background())
	require.NoError(t, err)

	pinotConn, ok := driverConn.(*pinotConn)
	require.True(t, ok)
	require.Equal(t, "baseballStats", pinotConn.defaultTable)

	require.NotNil(t, connector.Driver())
	_, err = pinotConn.Prepare("select 1")
	require.NoError(t, err)
	_, err = pinotConn.PrepareContext(context.Background(), "select 1")
	require.NoError(t, err)
	require.NoError(t, pinotConn.Close())
	_, err = pinotConn.Begin()
	require.ErrorIs(t, err, errReadOnly)
}

func TestIsReadQuery(t *testing.T) {
	require.True(t, isReadQuery("SELECT * FROM foo"))
	require.True(t, isReadQuery("with t as (select 1) select * from t"))
	require.True(t, isReadQuery("EXPLAIN PLAN FOR select * from foo"))
	require.True(t, isReadQuery("show tables"))
	require.False(t, isReadQuery("insert into foo values (1)"))
	require.False(t, isReadQuery(""))
}

func TestNamedValueConversionHelpers(t *testing.T) {
	values := valuesToNamed([]driver.Value{"a", 2})
	require.Len(t, values, 2)
	require.Equal(t, 1, values[0].Ordinal)
	require.Equal(t, "a", values[0].Value)

	interfaces := namedValuesToInterfaces(values)
	require.Equal(t, []interface{}{"a", 2}, interfaces)
}

func TestPinotConnExecContextReadOnly(t *testing.T) {
	conn := &pinotConn{}
	_, err := conn.ExecContext(context.Background(), "delete from foo", nil)
	require.ErrorIs(t, err, errReadOnly)
}

func TestPinotConnQueryContextCanceled(t *testing.T) {
	conn := &pinotConn{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := conn.QueryContext(ctx, "select * from foo", nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPinotConnExecContextCanceled(t *testing.T) {
	conn := &pinotConn{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := conn.ExecContext(ctx, "select * from foo", nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPinotStmtExecContextReadOnly(t *testing.T) {
	stmt := &pinotStmt{query: "update foo set bar = 1"}
	_, err := stmt.ExecContext(context.Background(), nil)
	require.ErrorIs(t, err, errReadOnly)
}

func TestPinotResultMethods(t *testing.T) {
	result := pinotResult{rowsAffected: 5}
	_, err := result.LastInsertId()
	require.ErrorIs(t, err, errReadOnly)
	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(5), rows)
}

func TestPinotConnQueryResultTable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"resultTable":{"dataSchema":{"columnDataTypes":["LONG","STRING"],"columnNames":["id","name"]},"rows":[[1,"alpha"]]},"exceptions":[]}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	conn, err := pinot.NewFromBrokerList([]string{server.URL})
	require.NoError(t, err)

	driverConn, err := newConnector(conn, "").Connect(context.Background())
	require.NoError(t, err)

	pConn, ok := driverConn.(*pinotConn)
	require.True(t, ok)
	stmt, err := pConn.Prepare("select * from baseballStats limit 1")
	require.NoError(t, err)

	stmtTyped, ok := stmt.(*pinotStmt)
	require.True(t, ok)

	rows, err := stmtTyped.QueryContext(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "name"}, rows.Columns())

	dest := make([]driver.Value, 2)
	require.NoError(t, rows.Next(dest))
	require.Equal(t, int64(1), dest[0])
	require.Equal(t, "alpha", dest[1])
	require.NoError(t, rows.Close())

	result, err := stmtTyped.ExecContext(context.Background(), nil)
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(0), affected)

	require.NoError(t, stmt.Close())
	require.Equal(t, -1, stmt.NumInput())
	require.NoError(t, pConn.Close())
}

func TestPinotConnQuerySelectionResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"selectionResults":{"columns":["id","name"],"results":[[2,"beta"]]},"exceptions":[]}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	conn, err := pinot.NewFromBrokerList([]string{server.URL})
	require.NoError(t, err)

	driverConn, err := newConnector(conn, "").Connect(context.Background())
	require.NoError(t, err)

	pConn, ok := driverConn.(*pinotConn)
	require.True(t, ok)

	rows, err := pConn.QueryContext(context.Background(), "select * from baseballStats", nil)
	require.NoError(t, err)

	dest := make([]driver.Value, 2)
	require.NoError(t, rows.Next(dest))
	require.Equal(t, int64(2), dest[0])
	require.Equal(t, "beta", dest[1])
	require.NoError(t, rows.Close())
}

func TestPinotConnQueryExceptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"exceptions":[{"message":"bad query"}]}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	conn, err := pinot.NewFromBrokerList([]string{server.URL})
	require.NoError(t, err)

	driverConn, err := newConnector(conn, "").Connect(context.Background())
	require.NoError(t, err)

	pConn, ok := driverConn.(*pinotConn)
	require.True(t, ok)

	_, err = pConn.QueryContext(context.Background(), "select * from baseballStats", nil)
	require.Error(t, err)
}

func TestPinotConnQueryMissingResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"exceptions":[]}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	conn, err := pinot.NewFromBrokerList([]string{server.URL})
	require.NoError(t, err)

	driverConn, err := newConnector(conn, "").Connect(context.Background())
	require.NoError(t, err)

	pConn, ok := driverConn.(*pinotConn)
	require.True(t, ok)

	_, err = pConn.QueryContext(context.Background(), "select * from baseballStats", nil)
	require.Error(t, err)
}

func TestPinotConnQueryExecuteError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	conn, err := pinot.NewFromBrokerList([]string{server.URL})
	require.NoError(t, err)

	driverConn, err := newConnector(conn, "").Connect(context.Background())
	require.NoError(t, err)

	pConn, ok := driverConn.(*pinotConn)
	require.True(t, ok)

	_, err = pConn.QueryContext(context.Background(), "select * from baseballStats", nil)
	require.Error(t, err)
}
