package pinot

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	log "github.com/sirupsen/logrus"
)

// BrokerResponse is the data structure for broker response.
type BrokerResponse struct {
	SelectionResults            *SelectionResults    `json:"SelectionResults,omitempty"`
	ResultTable                 *ResultTable         `json:"resultTable,omitempty"`
	TraceInfo                   map[string]string    `json:"traceInfo,omitempty"`
	AggregationResults          []*AggregationResult `json:"aggregationResults,omitempty"`
	Exceptions                  []Exception          `json:"exceptions"`
	NumSegmentsProcessed        int                  `json:"numSegmentsProcessed"`
	NumServersResponded         int                  `json:"numServersResponded"`
	NumSegmentsQueried          int                  `json:"numSegmentsQueried"`
	NumServersQueried           int                  `json:"numServersQueried"`
	NumSegmentsMatched          int                  `json:"numSegmentsMatched"`
	NumConsumingSegmentsQueried int                  `json:"numConsumingSegmentsQueried"`
	NumDocsScanned              int64                `json:"numDocsScanned"`
	NumEntriesScannedInFilter   int64                `json:"numEntriesScannedInFilter"`
	NumEntriesScannedPostFilter int64                `json:"numEntriesScannedPostFilter"`
	TotalDocs                   int64                `json:"totalDocs"`
	TimeUsedMs                  int                  `json:"timeUsedMs"`
	MinConsumingFreshnessTimeMs int64                `json:"minConsumingFreshnessTimeMs"`
	NumGroupsLimitReached       bool                 `json:"numGroupsLimitReached"`
}

// AggregationResult is the data structure for PQL aggregation result
type AggregationResult struct {
	Function       string       `json:"function"`
	Value          string       `json:"value,omitempty"`
	GroupByColumns []string     `json:"groupByColumns,omitempty"`
	GroupByResult  []GroupValue `json:"groupByResult,omitempty"`
}

// GroupValue is the data structure for PQL aggregation GroupBy result
type GroupValue struct {
	Value string   `json:"value"`
	Group []string `json:"group"`
}

// SelectionResults is the data structure for PQL selection result
type SelectionResults struct {
	Columns []string        `json:"columns"`
	Results [][]interface{} `json:"results"`
}

// RespSchema is response schema
type RespSchema struct {
	ColumnDataTypes []string `json:"columnDataTypes"`
	ColumnNames     []string `json:"columnNames"`
}

// Exception is Pinot exceptions.
type Exception struct {
	Message   string `json:"message"`
	ErrorCode int    `json:"errorCode"`
}

// ResultTable is a ResultTable
type ResultTable struct {
	DataSchema RespSchema      `json:"dataSchema"`
	Rows       [][]interface{} `json:"rows"`
}

// GetRowCount returns how many rows in the ResultTable
func (r ResultTable) GetRowCount() int {
	return len(r.Rows)
}

// GetColumnCount returns how many columns in the ResultTable
func (r ResultTable) GetColumnCount() int {
	return len(r.DataSchema.ColumnNames)
}

// GetColumnName returns column name given column index
func (r ResultTable) GetColumnName(columnIndex int) string {
	return r.DataSchema.ColumnNames[columnIndex]
}

// GetColumnDataType returns column data type given column index
func (r ResultTable) GetColumnDataType(columnIndex int) string {
	return r.DataSchema.ColumnDataTypes[columnIndex]
}

// Get returns a ResultTable entry given row index and column index
func (r ResultTable) Get(rowIndex int, columnIndex int) interface{} {
	return r.Rows[rowIndex][columnIndex]
}

// GetString returns a ResultTable string entry given row index and column index
func (r ResultTable) GetString(rowIndex int, columnIndex int) string {
	if col, ok := (r.Rows[rowIndex][columnIndex]).(string); ok {
		return col
	}
	if col, ok := (r.Rows[rowIndex][columnIndex]).(json.Number); ok {
		return string(col)
	}
	// Handle other common types by converting to string
	value := r.Rows[rowIndex][columnIndex]
	log.Debugf("Converting unexpected type %T to string at row %d, column %d: %v", value, rowIndex, columnIndex, value)
	return fmt.Sprintf("%v", value)
}

// isWithinInt32Range checks if a float64 value is within int32 range
func isWithinInt32Range(val float64) bool {
	return val <= float64(math.MaxInt32) && val >= float64(math.MinInt32)
}

// isWithinInt64Range checks if a float64 value is within int64 range
func isWithinInt64Range(val float64) bool {
	return val <= float64(math.MaxInt64) && val >= float64(math.MinInt64)
}

// isRangeError checks if an error is a strconv range error
func isRangeError(err error) bool {
	var rangeErr *strconv.NumError
	return errors.As(err, &rangeErr) && rangeErr.Err == strconv.ErrRange
}

// GetInt returns a ResultTable int entry given row index and column index
func (r ResultTable) GetInt(rowIndex int, columnIndex int) int32 {
	if col, ok := (r.Rows[rowIndex][columnIndex]).(json.Number); ok {
		val, err := col.Int64()
		if err != nil {
			// If Int64() failed, try parsing as float64 and converting to int32
			// This handles cases where numbers come back as "42.0" instead of "42"
			floatVal, floatErr := col.Float64()
			if floatErr != nil {
				log.Errorf("Error converting to float64: %v", floatErr)
				return 0
			}
			// Check if the float value is within int32 range
			if !isWithinInt32Range(floatVal) {
				log.Errorf("Error converting to int: value out of range: %f", floatVal)
				return 0
			}
			// Convert float to int32, checking for whole numbers
			if floatVal == float64(int32(floatVal)) {
				return int32(floatVal)
			}
			log.Errorf("Error converting to int: %v (value is not a whole number: %f)", err, floatVal)
			return 0
		}
		// Check if the value is within int32 range
		if val > int64(math.MaxInt32) || val < int64(math.MinInt32) {
			log.Errorf("Error converting to int: value out of range: %d", val)
			return 0
		}
		return int32(val)
	}
	log.Errorf("Error converting to json.Number at row %d, column %d: %v (type: %T)", rowIndex, columnIndex, r.Rows[rowIndex][columnIndex], r.Rows[rowIndex][columnIndex])
	return 0
}

// GetLong returns a ResultTable long entry given row index and column index
func (r ResultTable) GetLong(rowIndex int, columnIndex int) int64 {
	if col, ok := (r.Rows[rowIndex][columnIndex]).(json.Number); ok {
		val, err := col.Int64()
		if err != nil {
			// If Int64() failed, it could be either:
			// 1. A decimal number like "14859.0" that should be converted
			// 2. An out-of-range number that should return 0
			// We can differentiate by checking if the original Int64() error indicates overflow
			if isRangeError(err) {
				log.Errorf("Error converting to long: %v", err)
				return 0
			}

			// If Int64() failed for other reasons, try parsing as float64 and converting to int64
			// This handles cases where numbers come back as "14859.0" instead of "14859"
			floatVal, floatErr := col.Float64()
			if floatErr != nil {
				log.Errorf("Error converting to float64: %v", floatErr)
				return 0
			}
			// Check if the float value is within int64 range
			if !isWithinInt64Range(floatVal) {
				log.Errorf("Error converting to long: value out of range: %f", floatVal)
				return 0
			}
			// Convert float to int64, checking for whole numbers
			if floatVal == float64(int64(floatVal)) {
				return int64(floatVal)
			}
			log.Errorf("Error converting to long: %v (value is not a whole number: %f)", err, floatVal)
			return 0
		}
		return val
	}
	log.Errorf("Error converting to json.Number at row %d, column %d: %v (type: %T)", rowIndex, columnIndex, r.Rows[rowIndex][columnIndex], r.Rows[rowIndex][columnIndex])
	return 0
}

// GetFloat returns a ResultTable float entry given row index and column index
func (r ResultTable) GetFloat(rowIndex int, columnIndex int) float32 {
	if col, ok := (r.Rows[rowIndex][columnIndex]).(json.Number); ok {
		val, err := col.Float64()
		if err != nil {
			log.Errorf("Error converting to float: %v", err)
			return 0
		}
		// Check if the value is infinity (out of range for float32)
		if math.IsInf(val, 0) {
			log.Errorf("Error converting to float: value out of range (infinity): %f", val)
			return 0
		}
		// Check if the value is too large to fit in float32 (but not infinity)
		if val > float64(math.MaxFloat32) || val < -float64(math.MaxFloat32) {
			log.Errorf("Error converting to float: value out of range: %f", val)
			return 0
		}
		return float32(val)
	}
	log.Errorf("Error converting to json.Number at row %d, column %d: %v (type: %T)", rowIndex, columnIndex, r.Rows[rowIndex][columnIndex], r.Rows[rowIndex][columnIndex])
	return 0
}

// GetDouble returns a ResultTable double entry given row index and column index
func (r ResultTable) GetDouble(rowIndex int, columnIndex int) float64 {
	if col, ok := (r.Rows[rowIndex][columnIndex]).(json.Number); ok {
		val, err := col.Float64()
		if err != nil {
			log.Errorf("Error converting to double: %v", err)
			return 0
		}
		// Check if the value is infinity (out of range for float64)
		if math.IsInf(val, 0) {
			log.Errorf("Error converting to double: value out of range (infinity): %f", val)
			return 0
		}
		return val
	}
	log.Errorf("Error converting to json.Number at row %d, column %d: %v (type: %T)", rowIndex, columnIndex, r.Rows[rowIndex][columnIndex], r.Rows[rowIndex][columnIndex])
	return 0
}
