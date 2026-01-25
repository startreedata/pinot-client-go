package gormpinot

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"github.com/startreedata/pinot-client-go/pinot"
)

var errReadOnly = errors.New("pinot is read-only; write operations are not supported")

type connector struct {
	conn         *pinot.Connection
	defaultTable string
}

func newConnector(conn *pinot.Connection, defaultTable string) *connector {
	return &connector{conn: conn, defaultTable: defaultTable}
}

func (c *connector) Connect(context.Context) (driver.Conn, error) {
	return &pinotConn{conn: c.conn, defaultTable: c.defaultTable}, nil
}

func (c *connector) Driver() driver.Driver {
	return pinotDriver{}
}

type pinotDriver struct{}

func (pinotDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("pinot driver requires a Connector")
}

type pinotConn struct {
	conn         *pinot.Connection
	defaultTable string
}

func (c *pinotConn) Prepare(query string) (driver.Stmt, error) {
	return &pinotStmt{conn: c, query: query}, nil
}

func (c *pinotConn) Close() error {
	return nil
}

func (c *pinotConn) Begin() (driver.Tx, error) {
	return nil, errReadOnly
}

func (c *pinotConn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return c.Prepare(query)
}

func (c *pinotConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if !isReadQuery(query) {
		return nil, errReadOnly
	}
	_, err := c.query(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return pinotResult{rowsAffected: 0}, nil
}

func (c *pinotConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.query(ctx, query, args)
}

func (c *pinotConn) query(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
	values := namedValuesToInterfaces(args)
	table := c.defaultTable
	if table == "" {
		table = extractTableFromSQL(query)
	}
	resp, err := c.conn.ExecuteSQLWithParams(table, query, values)
	if err != nil {
		return nil, err
	}
	if len(resp.Exceptions) > 0 {
		return nil, fmt.Errorf("pinot query failed: %s", resp.Exceptions[0].Message)
	}
	if resp.ResultTable != nil {
		return newResultTableRows(resp.ResultTable), nil
	}
	if resp.SelectionResults != nil {
		return newSelectionRows(resp.SelectionResults), nil
	}
	return nil, errors.New("pinot response did not include a result set")
}

type pinotStmt struct {
	conn  *pinotConn
	query string
}

func (s *pinotStmt) Close() error {
	return nil
}

func (s *pinotStmt) NumInput() int {
	return -1
}

func (s *pinotStmt) Exec(args []driver.Value) (driver.Result, error) {
	if !isReadQuery(s.query) {
		return nil, errReadOnly
	}
	named := valuesToNamed(args)
	return s.conn.ExecContext(context.Background(), s.query, named)
}

func (s *pinotStmt) Query(args []driver.Value) (driver.Rows, error) {
	named := valuesToNamed(args)
	return s.conn.QueryContext(context.Background(), s.query, named)
}

type pinotResult struct {
	rowsAffected int64
}

func (r pinotResult) LastInsertId() (int64, error) {
	return 0, errReadOnly
}

func (r pinotResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func namedValuesToInterfaces(args []driver.NamedValue) []interface{} {
	if len(args) == 0 {
		return nil
	}
	values := make([]interface{}, 0, len(args))
	for _, arg := range args {
		values = append(values, arg.Value)
	}
	return values
}

func valuesToNamed(args []driver.Value) []driver.NamedValue {
	if len(args) == 0 {
		return nil
	}
	named := make([]driver.NamedValue, 0, len(args))
	for i, arg := range args {
		named = append(named, driver.NamedValue{Ordinal: i + 1, Value: arg})
	}
	return named
}

func isReadQuery(query string) bool {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return false
	}
	upper := strings.ToUpper(trimmed)
	return strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "WITH") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "SHOW")
}
