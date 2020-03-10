package pinot

type brokerSelector interface {
	init() error
	// Returns the broker address in the form host:port
	selectBroker(table string) (string, error)
}
