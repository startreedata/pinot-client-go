package gormpinot

import (
	"database/sql/driver"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/startreedata/pinot-client-go/pinot"
)

func TestResultTableRowsNext(t *testing.T) {
	result := &pinot.ResultTable{
		DataSchema: pinot.RespSchema{
			ColumnNames:     []string{"id", "score", "name"},
			ColumnDataTypes: []string{"LONG", "DOUBLE", "STRING"},
		},
		Rows: [][]interface{}{
			{json.Number("42"), json.Number("1.5"), "alpha"},
		},
	}

	rows := newResultTableRows(result)
	dest := make([]driver.Value, 3)

	err := rows.Next(dest)
	require.NoError(t, err)
	require.Equal(t, int64(42), dest[0])
	require.Equal(t, float64(1.5), dest[1])
	require.Equal(t, "alpha", dest[2])

	err = rows.Next(dest)
	require.Equal(t, io.EOF, err)
}

func TestResultTableRowsNextShortRow(t *testing.T) {
	result := &pinot.ResultTable{
		DataSchema: pinot.RespSchema{
			ColumnNames:     []string{"id", "name"},
			ColumnDataTypes: []string{"LONG"},
		},
		Rows: [][]interface{}{
			{json.Number("1")},
		},
	}

	rows := newResultTableRows(result)
	dest := make([]driver.Value, 2)
	require.NoError(t, rows.Next(dest))
	require.Equal(t, int64(1), dest[0])
	require.Nil(t, dest[1])
}

func TestResultTableRowsNextMissingColumnType(t *testing.T) {
	result := &pinot.ResultTable{
		DataSchema: pinot.RespSchema{
			ColumnNames:     []string{"id"},
			ColumnDataTypes: []string{},
		},
		Rows: [][]interface{}{
			{json.Number("1")},
		},
	}

	rows := newResultTableRows(result)
	dest := make([]driver.Value, 1)
	require.NoError(t, rows.Next(dest))
	require.Equal(t, int64(1), dest[0])
}

func TestResultTableRowsNextConvertError(t *testing.T) {
	result := &pinot.ResultTable{
		DataSchema: pinot.RespSchema{
			ColumnNames:     []string{"id"},
			ColumnDataTypes: []string{"LONG"},
		},
		Rows: [][]interface{}{
			{uint64(^uint64(0))},
		},
	}

	rows := newResultTableRows(result)
	dest := make([]driver.Value, 1)
	err := rows.Next(dest)
	require.Error(t, err)
}

func TestSelectionRowsNext(t *testing.T) {
	result := &pinot.SelectionResults{
		Columns: []string{"id", "name"},
		Results: [][]interface{}{
			{json.Number("7"), "delta"},
		},
	}

	rows := newSelectionRows(result)
	dest := make([]driver.Value, 2)

	err := rows.Next(dest)
	require.NoError(t, err)
	require.Equal(t, int64(7), dest[0])
	require.Equal(t, "delta", dest[1])

	err = rows.Next(dest)
	require.Equal(t, io.EOF, err)
}

func TestResultRowsColumnsAndClose(t *testing.T) {
	result := &pinot.ResultTable{
		DataSchema: pinot.RespSchema{
			ColumnNames:     []string{"id", "name"},
			ColumnDataTypes: []string{"LONG", "STRING"},
		},
		Rows: [][]interface{}{
			{json.Number("1"), "alpha"},
		},
	}

	rows := newResultTableRows(result)
	require.Equal(t, []string{"id", "name"}, rows.Columns())
	require.NoError(t, rows.Close())
}

func TestSelectionRowsColumnsAndClose(t *testing.T) {
	result := &pinot.SelectionResults{
		Columns: []string{"id", "name"},
		Results: [][]interface{}{
			{json.Number("1"), "alpha"},
		},
	}

	rows := newSelectionRows(result)
	require.Equal(t, []string{"id", "name"}, rows.Columns())
	require.NoError(t, rows.Close())
}

func TestExtractTableFromSQL(t *testing.T) {
	require.Equal(t, "baseballStats", extractTableFromSQL("select * from baseballStats limit 1"))
	require.Equal(t, "baseballStats", extractTableFromSQL("SELECT * FROM `baseballStats`"))
	require.Equal(t, "baseballStats", extractTableFromSQL(`select * from "baseballStats"`))
	require.Equal(t, "", extractTableFromSQL("select 1"))
}

func TestConvertValueUintOverflow(t *testing.T) {
	value, err := convertValue(uint64(^uint64(0)), "LONG")
	require.Error(t, err)
	require.Nil(t, value)

	value, err = convertValue(^uint(0), "LONG")
	require.Error(t, err)
	require.Nil(t, value)
}

func TestConvertValueTypes(t *testing.T) {
	value, err := convertValue(nil, "LONG")
	require.NoError(t, err)
	require.Nil(t, value)

	value, err = convertValue(uint(7), "LONG")
	require.NoError(t, err)
	require.Equal(t, int64(7), value)

	value, err = convertValue(int(9), "LONG")
	require.NoError(t, err)
	require.Equal(t, int64(9), value)

	value, err = convertValue(uint8(8), "LONG")
	require.NoError(t, err)
	require.Equal(t, int64(8), value)

	value, err = convertValue(uint16(16), "LONG")
	require.NoError(t, err)
	require.Equal(t, int64(16), value)

	value, err = convertValue(uint32(32), "LONG")
	require.NoError(t, err)
	require.Equal(t, int64(32), value)

	value, err = convertValue(uint64(64), "LONG")
	require.NoError(t, err)
	require.Equal(t, int64(64), value)

	value, err = convertValue(int8(1), "LONG")
	require.NoError(t, err)
	require.Equal(t, int64(1), value)

	value, err = convertValue(int16(2), "LONG")
	require.NoError(t, err)
	require.Equal(t, int64(2), value)

	value, err = convertValue(int32(3), "LONG")
	require.NoError(t, err)
	require.Equal(t, int64(3), value)

	value, err = convertValue(int64(4), "LONG")
	require.NoError(t, err)
	require.Equal(t, int64(4), value)

	value, err = convertValue(float32(1.25), "FLOAT")
	require.NoError(t, err)
	require.Equal(t, float64(1.25), value)

	value, err = convertValue(float64(2.5), "DOUBLE")
	require.NoError(t, err)
	require.Equal(t, float64(2.5), value)

	value, err = convertValue(json.Number("42"), "INT")
	require.NoError(t, err)
	require.Equal(t, int64(42), value)

	value, err = convertValue(json.Number("3.14"), "DOUBLE")
	require.NoError(t, err)
	require.Equal(t, float64(3.14), value)

	value, err = convertValue(true, "BOOLEAN")
	require.NoError(t, err)
	require.Equal(t, true, value)

	value, err = convertValue([]byte("raw"), "BYTES")
	require.NoError(t, err)
	require.Equal(t, []byte("raw"), value)

	value, err = convertValue("data", "BYTES")
	require.NoError(t, err)
	require.Equal(t, []byte("data"), value)

	value, err = convertValue("text", "STRING")
	require.NoError(t, err)
	require.Equal(t, "text", value)

	value, err = convertValue(struct{ X int }{X: 1}, "UNKNOWN")
	require.NoError(t, err)
	require.Equal(t, "{1}", value)
}

func TestConvertJSONNumberBranches(t *testing.T) {
	value, err := convertJSONNumber(json.Number("7"), "INT")
	require.NoError(t, err)
	require.Equal(t, int64(7), value)

	value, err = convertJSONNumber(json.Number("1.5"), "INT")
	require.NoError(t, err)
	require.Equal(t, float64(1.5), value)

	value, err = convertJSONNumber(json.Number("2.5"), "DOUBLE")
	require.NoError(t, err)
	require.Equal(t, float64(2.5), value)

	value, err = convertJSONNumber(json.Number("bad"), "DOUBLE")
	require.NoError(t, err)
	require.Equal(t, "bad", value)
}

func TestExtractTableFromSQLEdgeCases(t *testing.T) {
	require.Equal(t, "", extractTableFromSQL("select * from "))
	require.Equal(t, "", extractTableFromSQL(`select * from ""`))
}
