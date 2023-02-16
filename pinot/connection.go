package pinot

import (
	log "github.com/sirupsen/logrus"
)

// Connection to Pinot, normally created through calls to the {@link ConnectionFactory}.
type Connection struct {
	transport      clientTransport
	brokerSelector brokerSelector
	trace          bool
}

// ExecuteSQL for a given table
func (c *Connection) ExecuteSQL(table string, query string) (*BrokerResponse, error) {
	brokerAddress, err := c.brokerSelector.selectBroker(table)
	if err != nil {
		log.Errorf("Unable to find an available broker for table %s, Error: %v\n", table, err)
		return nil, err
	}
	brokerResp, err := c.transport.execute(brokerAddress, &Request{
		queryFormat: "sql",
		query:       query,
		trace:       c.trace,
	})
	if err != nil {
		log.Errorf("Caught exception to execute SQL query %s, Error: %v\n", query, err)
		return nil, err
	}
	return brokerResp, err
}

func (c *Connection) OpenTrace() {
	c.trace = true
}

func (c *Connection) CloseTrace() {
	c.trace = false
}
