package gormpinot

import (
	"database/sql/driver"
	"encoding/json"
	"io"
	"testing"

	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/stretchr/testify/require"
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
