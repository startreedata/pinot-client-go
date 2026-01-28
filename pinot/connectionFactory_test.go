package pinot

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWithConfigAndClientUsesDefaultClient(t *testing.T) {
	conn, err := NewWithConfigAndClient(&ClientConfig{
		BrokerList: []string{"localhost:8000"},
	}, nil)
	require.NoError(t, err)

	transport, ok := conn.transport.(*jsonAsyncHTTPClientTransport)
	require.True(t, ok)
	require.Same(t, http.DefaultClient, transport.client)
}

func TestNewWithConfigAndClientUsesProvidedClient(t *testing.T) {
	custom := &http.Client{}
	conn, err := NewWithConfigAndClient(&ClientConfig{
		BrokerList: []string{"localhost:8000"},
	}, custom)
	require.NoError(t, err)

	transport, ok := conn.transport.(*jsonAsyncHTTPClientTransport)
	require.True(t, ok)
	require.Same(t, custom, transport.client)
}

func TestNewWithConfigAndClientCopiesTimeout(t *testing.T) {
	baseClient := &http.Client{Timeout: 2 * time.Second}
	conn, err := NewWithConfigAndClient(&ClientConfig{
		BrokerList:  []string{"localhost:8000"},
		HTTPTimeout: 5 * time.Second,
	}, baseClient)
	require.NoError(t, err)

	require.Equal(t, 2*time.Second, baseClient.Timeout)
	transport, ok := conn.transport.(*jsonAsyncHTTPClientTransport)
	require.True(t, ok)
	require.Equal(t, 5*time.Second, transport.client.Timeout)
	require.NotSame(t, baseClient, transport.client)
}

func TestNewWithConfigAndClientNilClientWithTimeout(t *testing.T) {
	defaultTimeout := http.DefaultClient.Timeout
	conn, err := NewWithConfigAndClient(&ClientConfig{
		BrokerList:  []string{"localhost:8000"},
		HTTPTimeout: 3 * time.Second,
	}, nil)
	require.NoError(t, err)

	transport, ok := conn.transport.(*jsonAsyncHTTPClientTransport)
	require.True(t, ok)
	require.NotSame(t, http.DefaultClient, transport.client)
	require.Equal(t, defaultTimeout, http.DefaultClient.Timeout)
}

func TestNewWithConfigAndClientControllerConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"baseballStats":[{"host":"localhost","port":8000,"instanceName":"Broker_1"}]}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	conn, err := NewWithConfigAndClient(&ClientConfig{
		ControllerConfig: &ControllerConfig{
			ControllerAddress: server.URL,
		},
	}, &http.Client{})
	require.NoError(t, err)

	selector, ok := conn.brokerSelector.(*controllerBasedSelector)
	require.True(t, ok)
	require.NotNil(t, selector.client)
}

func TestNewWithConfigAndClientGrpcTransport(t *testing.T) {
	conn, err := NewWithConfigAndClient(&ClientConfig{
		BrokerList: []string{"localhost:8000"},
		GrpcConfig: &GrpcConfig{
			Encoding:     "JSON",
			Compression:  "NONE",
			BlockRowSize: 1,
		},
	}, nil)
	require.NoError(t, err)

	_, ok := conn.transport.(*grpcBrokerClientTransport)
	require.True(t, ok)
}

func TestNewWithConfigAndClientGrpcTransportError(t *testing.T) {
	original := grpcTransportFactory
	grpcTransportFactory = func(_ *GrpcConfig) (*grpcBrokerClientTransport, error) {
		return nil, errors.New("transport error")
	}
	t.Cleanup(func() { grpcTransportFactory = original })

	_, err := NewWithConfigAndClient(&ClientConfig{
		BrokerList: []string{"localhost:8000"},
		GrpcConfig: &GrpcConfig{
			Encoding:     "JSON",
			Compression:  "NONE",
			BlockRowSize: 1,
		},
	}, nil)
	assert.Error(t, err)
}

func TestPinotClients(t *testing.T) {
	pinotClient1, err := NewFromZookeeper([]string{"localhost:12181"}, "", "QuickStartCluster")
	assert.NotNil(t, pinotClient1)
	assert.NotNil(t, pinotClient1.brokerSelector)
	assert.NotNil(t, pinotClient1.transport)
	// Since there is no zk setup, so an error will be raised
	assert.NotNil(t, err)
	pinotClient2, err := NewWithConfig(&ClientConfig{
		ZkConfig: &ZookeeperConfig{
			ZookeeperPath:     []string{"localhost:12181"},
			PathPrefix:        strings.Join([]string{"/", "QuickStartCluster"}, "/"),
			SessionTimeoutSec: defaultZkSessionTimeoutSec,
		},
		ExtraHTTPHeader: map[string]string{
			"k1": "v1",
		},
	})
	assert.NotNil(t, pinotClient2)
	assert.NotNil(t, pinotClient2.brokerSelector)
	assert.NotNil(t, pinotClient2.transport)
	// Since there is no zk setup, so an error will be raised
	assert.NotNil(t, err)
	pinotClient3, err := NewFromController("localhost:19000")
	assert.NotNil(t, pinotClient3)
	assert.NotNil(t, pinotClient3.brokerSelector)
	assert.NotNil(t, pinotClient3.transport)
	// Since there is no controller setup, so an error will be raised
	assert.NotNil(t, err)
	_, err = NewWithConfig(&ClientConfig{})
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "please specify"))
	pinotClient4, err := NewWithConfig(&ClientConfig{
		ZkConfig: &ZookeeperConfig{
			ZookeeperPath:     []string{"localhost:12181"},
			PathPrefix:        strings.Join([]string{"/", "QuickStartCluster"}, "/"),
			SessionTimeoutSec: defaultZkSessionTimeoutSec,
		},
		ExtraHTTPHeader: map[string]string{
			"k1": "v1",
		},
		UseMultistageEngine: true,
	})
	assert.NotNil(t, pinotClient4)
	assert.NotNil(t, pinotClient4.brokerSelector)
	assert.NotNil(t, pinotClient4.transport)
	assert.True(t, pinotClient4.useMultistageEngine)
	// Since there is no zk setup, so an error will be raised
	assert.NotNil(t, err)
	pinotClient5, err := NewWithConfig(&ClientConfig{
		ZkConfig: &ZookeeperConfig{
			ZookeeperPath:     []string{"localhost:12181"},
			PathPrefix:        strings.Join([]string{"/", "QuickStartCluster"}, "/"),
			SessionTimeoutSec: defaultZkSessionTimeoutSec,
		},
		ExtraHTTPHeader: map[string]string{
			"k1": "v1",
		},
	})
	pinotClient5.UseMultistageEngine(true)
	assert.NotNil(t, pinotClient5)
	assert.NotNil(t, pinotClient5.brokerSelector)
	assert.NotNil(t, pinotClient5.transport)
	assert.True(t, pinotClient5.useMultistageEngine)
	// Since there is no zk setup, so an error will be raised
	assert.NotNil(t, err)
}

func TestPinotWithHttpTimeout(t *testing.T) {
	pinotClient, err := NewWithConfig(&ClientConfig{
		// Hit an unreachable port
		BrokerList: []string{"www.google.com:81"},
		// Set timeout to 1 sec
		HTTPTimeout: 1 * time.Second,
	})
	assert.Nil(t, err)
	start := time.Now()
	_, err = pinotClient.ExecuteSQL("testTable", "select * from testTable")
	end := time.Since(start)
	assert.NotNil(t, err)
	diff := int(end.Seconds())
	// Query should ideally timeout in 1 sec, considering other variables,
	// diff might not be exactly equal to 1. So, we can assert that diff
	// must be less than 2 sec.
	assert.Less(t, diff, 2)
}
