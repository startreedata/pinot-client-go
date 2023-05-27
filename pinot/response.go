package pinot

import "encoding/json"

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
	return (r.Rows[rowIndex][columnIndex]).(string)
}

// GetInt returns a ResultTable int entry given row index and column index
func (r ResultTable) GetInt(rowIndex int, columnIndex int) int32 {
	val, _ := (r.Rows[rowIndex][columnIndex]).(json.Number).Int64()
	return int32(val)
}

// GetLong returns a ResultTable long entry given row index and column index
func (r ResultTable) GetLong(rowIndex int, columnIndex int) int64 {
	val, _ := (r.Rows[rowIndex][columnIndex]).(json.Number).Int64()
	return val
}

// GetFloat returns a ResultTable float entry given row index and column index
func (r ResultTable) GetFloat(rowIndex int, columnIndex int) float32 {
	val, _ := (r.Rows[rowIndex][columnIndex]).(json.Number).Float64()
	return float32(val)
}

// GetDouble returns a ResultTable double entry given row index and column index
func (r ResultTable) GetDouble(rowIndex int, columnIndex int) float64 {
	val, _ := (r.Rows[rowIndex][columnIndex]).(json.Number).Float64()
	return val
}
