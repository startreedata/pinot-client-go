package gormpinot

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/startreedata/pinot-client-go/pinot"
)

type resultRows struct {
	columns     []string
	columnTypes []string
	rows        [][]interface{}
	index       int
}

func newResultTableRows(result *pinot.ResultTable) *resultRows {
	return &resultRows{
		columns:     result.DataSchema.ColumnNames,
		columnTypes: result.DataSchema.ColumnDataTypes,
		rows:        result.Rows,
		index:       0,
	}
}

func newSelectionRows(result *pinot.SelectionResults) *resultRows {
	return &resultRows{
		columns: result.Columns,
		rows:    result.Results,
		index:   0,
	}
}

func (r *resultRows) Columns() []string {
	return r.columns
}

func (r *resultRows) Close() error {
	return nil
}

func (r *resultRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.index]
	r.index++

	for i := range dest {
		if i >= len(row) {
			dest[i] = nil
			continue
		}
		columnType := ""
		if i < len(r.columnTypes) {
			columnType = r.columnTypes[i]
		}
		converted, err := convertValue(row[i], columnType)
		if err != nil {
			return err
		}
		dest[i] = converted
	}
	return nil
}

func convertValue(value interface{}, columnType string) (driver.Value, error) {
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case json.Number:
		return convertJSONNumber(v, columnType)
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case bool:
		return v, nil
	case []byte:
		return v, nil
	case string:
		if isBytesType(columnType) {
			return []byte(v), nil
		}
		return v, nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func convertJSONNumber(value json.Number, columnType string) (driver.Value, error) {
	if isIntType(columnType) {
		if v, err := value.Int64(); err == nil {
			return v, nil
		}
	}
	if isFloatType(columnType) {
		if v, err := value.Float64(); err == nil {
			return v, nil
		}
	}
	if v, err := value.Float64(); err == nil {
		if v == float64(int64(v)) {
			return int64(v), nil
		}
		return v, nil
	}
	return value.String(), nil
}

func isIntType(columnType string) bool {
	switch strings.ToUpper(columnType) {
	case "INT", "LONG", "TIMESTAMP":
		return true
	default:
		return false
	}
}

func isFloatType(columnType string) bool {
	switch strings.ToUpper(columnType) {
	case "FLOAT", "DOUBLE", "BIG_DECIMAL":
		return true
	default:
		return false
	}
}

func isBytesType(columnType string) bool {
	return strings.EqualFold(columnType, "BYTES")
}

func extractTableFromSQL(query string) string {
	lower := strings.ToLower(query)
	fromIdx := strings.Index(lower, " from ")
	if fromIdx == -1 {
		return ""
	}
	fragment := strings.TrimSpace(query[fromIdx+6:])
	if fragment == "" {
		return ""
	}
	parts := strings.Fields(fragment)
	if len(parts) == 0 {
		return ""
	}
	table := strings.Trim(parts[0], "`\"")
	if table == "" {
		return ""
	}
	return table
}
