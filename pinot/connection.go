package pinot

import (
	"fmt"
	"math/big"
	"strings"
	"time"
)

// Connection to Pinot, normally created through calls to the {@link ConnectionFactory}.
type Connection struct {
	transport           clientTransport
	brokerSelector      brokerSelector
	trace               bool
	useMultistageEngine bool
}

// UseMultistageEngine for the connection
func (c *Connection) UseMultistageEngine(useMultistageEngine bool) {
	c.useMultistageEngine = useMultistageEngine
}

// ExecuteSQL for a given table
func (c *Connection) ExecuteSQL(table string, query string) (*BrokerResponse, error) {
	brokerAddress, err := c.brokerSelector.selectBroker(table)
	if err != nil {
		return nil, fmt.Errorf("unable to find an available broker for table %s, Error: %v", table, err)
	}
	brokerResp, err := c.transport.execute(brokerAddress, &Request{
		queryFormat:         "sql",
		query:               query,
		trace:               c.trace,
		useMultistageEngine: c.useMultistageEngine,
	})
	if err != nil {
		return nil, fmt.Errorf("caught exception to execute SQL query %s, Error: %v", query, err)
	}
	return brokerResp, err
}

// ExecuteSQLWithParams executes an SQL query with parameters for a given table
func (c *Connection) ExecuteSQLWithParams(table string, queryPattern string, params []interface{}) (*BrokerResponse, error) {
	query, err := formatQuery(queryPattern, params)
	if err != nil {
		return nil, fmt.Errorf("failed to format query: %v", err)
	}
	return c.ExecuteSQL(table, query)
}

func formatQuery(queryPattern string, params []interface{}) (string, error) {
	// Count the number of placeholders in queryPattern
	numPlaceholders := strings.Count(queryPattern, "?")
	if numPlaceholders != len(params) {
		return "", fmt.Errorf("number of placeholders in queryPattern (%d) does not match number of params (%d)", numPlaceholders, len(params))
	}

	// Split the query by '?' and incrementally build the new query
	parts := strings.Split(queryPattern, "?")

	var newQuery strings.Builder
	for i, part := range parts[:len(parts)-1] {
		newQuery.WriteString(part)
		formattedParam, err := formatArg(params[i])
		if err != nil {
			return "", fmt.Errorf("failed to format parameter: %v", err)
		}
		newQuery.WriteString(formattedParam)
	}
	// Add the last part of the query, which does not follow a '?'
	newQuery.WriteString(parts[len(parts)-1])
	return newQuery.String(), nil
}

func formatArg(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		// For pinot type - STRING - enclose in single quotes
		return escapeStringValue(v), nil
	case *big.Int, *big.Float:
		// For pinot types - BIG_DECIMAL and BYTES - enclose in single quotes
		return fmt.Sprintf("'%v'", v), nil
	case []byte:
		// For pinot type - BYTES - convert to Hex string and enclose in single quotes
		hexString := fmt.Sprintf("%x", v)
		return fmt.Sprintf("'%s'", hexString), nil
	case time.Time:
		// For pinot type - TIMESTAMP - convert to ISO8601 format and enclose in single quotes
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05.000")), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		// For types - INT, LONG, FLOAT, DOUBLE and BOOLEAN use as-is
		return fmt.Sprintf("%v", v), nil
	default:
		// Throw error for unsupported types
		return "", fmt.Errorf("unsupported type: %T", v)
	}
}

func escapeStringValue(s string) string {
	return fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
}

// OpenTrace for the connection
func (c *Connection) OpenTrace() {
	c.trace = true
}

// CloseTrace for the connection
func (c *Connection) CloseTrace() {
	c.trace = false
}
