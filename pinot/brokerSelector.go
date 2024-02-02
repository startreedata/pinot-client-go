// Package pinot provides a client for Pinot, a real-time distributed OLAP datastore.
package pinot

type brokerSelector interface {
	init() error
	// Returns the broker address in the form host:port
	selectBroker(table string) (string, error)
}
