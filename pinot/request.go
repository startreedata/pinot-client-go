package pinot

// Request is used in server request to host multiple pinot query types, like PQL, SQL.
type Request struct {
	queryFormat string
	query       string
	trace       bool
}
