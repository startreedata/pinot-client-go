package pinot

import (
	"fmt"
	"strings"
	"sync"
)

// PreparedStatement represents a prepared statement with bind variables that can be executed multiple times
// with different parameter values. It's similar to database/sql.Stmt but adapted for Pinot.
type PreparedStatement interface {
	// SetString sets the parameter at the given index to the given string value
	SetString(parameterIndex int, value string) error

	// SetInt sets the parameter at the given index to the given int value
	SetInt(parameterIndex int, value int) error

	// SetInt64 sets the parameter at the given index to the given int64 value
	SetInt64(parameterIndex int, value int64) error

	// SetFloat64 sets the parameter at the given index to the given float64 value
	SetFloat64(parameterIndex int, value float64) error

	// SetBool sets the parameter at the given index to the given bool value
	SetBool(parameterIndex int, value bool) error

	// Set sets the parameter at the given index to the given value (any supported type)
	Set(parameterIndex int, value interface{}) error

	// Execute executes the prepared statement with the currently set parameters
	Execute() (*BrokerResponse, error)

	// ExecuteWithParams executes the prepared statement with the given parameters
	// This is a convenience method that sets all parameters and executes in one call
	ExecuteWithParams(params ...interface{}) (*BrokerResponse, error)

	// GetQuery returns the original query template
	GetQuery() string

	// GetParameterCount returns the number of parameters in the prepared statement
	GetParameterCount() int

	// ClearParameters clears all currently set parameters
	ClearParameters() error

	// Close closes the prepared statement and releases any associated resources
	Close() error
}

// preparedStatement is the concrete implementation of PreparedStatement
type preparedStatement struct {
	connection    *Connection
	table         string
	queryTemplate string
	queryParts    []string // Query split by '?' placeholders
	paramCount    int
	parameters    []interface{}
	mutex         sync.RWMutex
	closed        bool
}

// Prepare creates a new PreparedStatement for the given table and query template.
// The query template should use '?' as placeholders for parameters.
// Example: "SELECT * FROM table WHERE column1 = ? AND column2 = ?"
func (c *Connection) Prepare(table string, queryTemplate string) (PreparedStatement, error) {
	if table == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}
	if queryTemplate == "" {
		return nil, fmt.Errorf("query template cannot be empty")
	}

	// Split the query by '?' to prepare for parameter substitution
	parts := strings.Split(queryTemplate, "?")
	paramCount := len(parts) - 1

	if paramCount == 0 {
		return nil, fmt.Errorf("query template must contain at least one parameter placeholder (?)")
	}

	return &preparedStatement{
		connection:    c,
		table:         table,
		queryTemplate: queryTemplate,
		queryParts:    parts,
		paramCount:    paramCount,
		parameters:    make([]interface{}, paramCount),
		closed:        false,
	}, nil
}

// SetString sets the parameter at the given index to the given string value
func (ps *preparedStatement) SetString(parameterIndex int, value string) error {
	return ps.Set(parameterIndex, value)
}

// SetInt sets the parameter at the given index to the given int value
func (ps *preparedStatement) SetInt(parameterIndex int, value int) error {
	return ps.Set(parameterIndex, value)
}

// SetInt64 sets the parameter at the given index to the given int64 value
func (ps *preparedStatement) SetInt64(parameterIndex int, value int64) error {
	return ps.Set(parameterIndex, value)
}

// SetFloat64 sets the parameter at the given index to the given float64 value
func (ps *preparedStatement) SetFloat64(parameterIndex int, value float64) error {
	return ps.Set(parameterIndex, value)
}

// SetBool sets the parameter at the given index to the given bool value
func (ps *preparedStatement) SetBool(parameterIndex int, value bool) error {
	return ps.Set(parameterIndex, value)
}

// Set sets the parameter at the given index to the given value (any supported type)
func (ps *preparedStatement) Set(parameterIndex int, value interface{}) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	if ps.closed {
		return fmt.Errorf("prepared statement is closed")
	}

	if parameterIndex < 1 || parameterIndex > ps.paramCount {
		return fmt.Errorf("parameter index %d is out of range [1, %d]", parameterIndex, ps.paramCount)
	}

	// Convert to 0-based index
	ps.parameters[parameterIndex-1] = value
	return nil
}

// Execute executes the prepared statement with the currently set parameters
func (ps *preparedStatement) Execute() (*BrokerResponse, error) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	if ps.closed {
		return nil, fmt.Errorf("prepared statement is closed")
	}

	// Check if all parameters are set
	for i, param := range ps.parameters {
		if param == nil {
			return nil, fmt.Errorf("parameter at index %d is not set", i+1)
		}
	}

	// Build the final query
	query, err := ps.buildQuery(ps.parameters)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %v", err)
	}

	// Execute the query using the connection
	return ps.connection.ExecuteSQL(ps.table, query)
}

// ExecuteWithParams executes the prepared statement with the given parameters
// This is a convenience method that sets all parameters and executes in one call
func (ps *preparedStatement) ExecuteWithParams(params ...interface{}) (*BrokerResponse, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	if ps.closed {
		return nil, fmt.Errorf("prepared statement is closed")
	}

	if len(params) != ps.paramCount {
		return nil, fmt.Errorf("expected %d parameters, got %d", ps.paramCount, len(params))
	}

	// Build the final query
	query, err := ps.buildQuery(params)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %v", err)
	}

	// Execute the query using the connection
	return ps.connection.ExecuteSQL(ps.table, query)
}

// GetQuery returns the original query template
func (ps *preparedStatement) GetQuery() string {
	return ps.queryTemplate
}

// GetParameterCount returns the number of parameters in the prepared statement
func (ps *preparedStatement) GetParameterCount() int {
	return ps.paramCount
}

// ClearParameters clears all currently set parameters
func (ps *preparedStatement) ClearParameters() error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	if ps.closed {
		return fmt.Errorf("prepared statement is closed")
	}

	for i := range ps.parameters {
		ps.parameters[i] = nil
	}
	return nil
}

// Close closes the prepared statement and releases any associated resources
func (ps *preparedStatement) Close() error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	ps.closed = true
	ps.parameters = nil
	return nil
}

// buildQuery builds the final SQL query by substituting parameters
func (ps *preparedStatement) buildQuery(params []interface{}) (string, error) {
	if len(params) != ps.paramCount {
		return "", fmt.Errorf("expected %d parameters, got %d", ps.paramCount, len(params))
	}

	var query strings.Builder
	for i, part := range ps.queryParts[:len(ps.queryParts)-1] {
		query.WriteString(part)
		formattedParam, err := formatArg(params[i])
		if err != nil {
			return "", fmt.Errorf("failed to format parameter at index %d: %v", i+1, err)
		}
		query.WriteString(formattedParam)
	}
	// Add the last part of the query, which does not follow a '?'
	query.WriteString(ps.queryParts[len(ps.queryParts)-1])
	return query.String(), nil
}
