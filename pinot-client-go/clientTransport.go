package pinot

type clientTransport interface {
	execute(brokerAddress string, query *Request) (*BrokerResponse, error)
}
