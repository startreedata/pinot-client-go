package pinot

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type errBody struct{}

func (errBody) Read(_ []byte) (int, error) {
	return 0, assert.AnError
}

func (errBody) Close() error {
	return nil
}

func TestGetQueryTemplate(t *testing.T) {
	assert.Equal(t, "http://%s/query/sql", getQueryTemplate("sql", "localhost:8000"))
	assert.Equal(t, "http://%s/query", getQueryTemplate("pql", "localhost:8000"))
	assert.Equal(t, "%s/query/sql", getQueryTemplate("sql", "http://localhost:8000"))
	assert.Equal(t, "%s/query", getQueryTemplate("pql", "http://localhost:8000"))
	assert.Equal(t, "%s/query/sql", getQueryTemplate("sql", "https://localhost:8000"))
	assert.Equal(t, "%s/query", getQueryTemplate("pql", "https://localhost:8000"))
}

func TestCreateHTTPRequest(t *testing.T) {
	r, err := createHTTPRequest("localhost:8000", []byte(`{"sql": "select * from baseballStats limit 10"}`), map[string]string{"a": "b"})
	assert.Nil(t, err)
	assert.Equal(t, "POST", r.Method)
	_, err = createHTTPRequest("localhos\t:8000", []byte(`{"sql": "select * from baseballStats limit 10"}`), map[string]string{"a": "b"})
	assert.NotNil(t, err)
}

func TestCreateHTTPRequestWithTrace(t *testing.T) {
	r, err := createHTTPRequest("localhost:8000", []byte(`{"sql": "select * from baseballStats limit 10", "trace": "true"}`), map[string]string{"a": "b"})
	assert.Nil(t, err)
	assert.Equal(t, "POST", r.Method)
	_, err = createHTTPRequest("localhos\t:8000", []byte(`{"sql": "select * from baseballStats limit 10", "trace": "true"}`), map[string]string{"a": "b"})
	assert.NotNil(t, err)
}

func TestJsonAsyncHTTPClientTransport(t *testing.T) {
	transport := &jsonAsyncHTTPClientTransport{
		client: http.DefaultClient,
		header: map[string]string{"a": "b"},
	}
	_, err := transport.execute("localhos\t:8000", &Request{
		queryFormat: "sql",
		query:       "select * from baseballStats limit 10",
	})
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "parse "))

	_, err = transport.execute("randomhost", &Request{
		queryFormat: "sql",
		query:       "select * from baseballStats limit 10",
	})
	assert.NotNil(t, err)

	_, err = transport.execute("localhost:18000", &Request{
		queryFormat:         "sql",
		query:               "select * from baseballStats limit 10",
		useMultistageEngine: true,
	})
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Post "))
}

func TestJsonAsyncHTTPClientTransportNonOKResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	transport := &jsonAsyncHTTPClientTransport{
		client: server.Client(),
		header: map[string]string{},
	}

	_, err := transport.execute(server.URL, &Request{
		queryFormat: "sql",
		query:       "select * from baseballStats limit 1",
	})
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "http exception"))
}

func TestJsonAsyncHTTPClientTransportTraceAndOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["id"]},"rows":[[1]]},"exceptions":[]}`))
		assert.NoError(t, err)
	}))
	defer server.Close()

	transport := &jsonAsyncHTTPClientTransport{
		client: &http.Client{Timeout: 500 * time.Millisecond},
		header: map[string]string{"x-test": "1"},
	}

	_, err := transport.execute(server.URL, &Request{
		queryFormat:         "sql",
		query:               "select * from baseballStats limit 1",
		useMultistageEngine: true,
		trace:               true,
	})
	assert.NoError(t, err)
}

func TestJsonAsyncHTTPClientTransportReadError(t *testing.T) {
	client := &http.Client{
		Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       errBody{},
			}, nil
		}),
	}

	transport := &jsonAsyncHTTPClientTransport{
		client: client,
		header: map[string]string{},
	}

	_, err := transport.execute("http://example.com", &Request{
		queryFormat: "sql",
		query:       "select * from baseballStats limit 1",
	})
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "unable to read Pinot response"))
}

func TestBuildQueryOptions(t *testing.T) {
	transport := &jsonAsyncHTTPClientTransport{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		header: map[string]string{"a": "b"},
	}
	assert.Equal(t, "groupByMode=sql;responseFormat=sql;timeoutMs=10000", transport.buildQueryOptions(&Request{
		queryFormat: "sql",
		query:       "select * from baseballStats limit 10",
	}))
	assert.Equal(t, "groupByMode=sql;responseFormat=sql;useMultistageEngine=true;timeoutMs=10000", transport.buildQueryOptions(&Request{
		queryFormat:         "sql",
		query:               "select * from baseballStats limit 10",
		useMultistageEngine: true,
	}))

	transport = &jsonAsyncHTTPClientTransport{
		client: &http.Client{},
		header: map[string]string{"a": "b"},
	}

	// should not have timeoutMs
	assert.Equal(t, "", transport.buildQueryOptions(&Request{
		queryFormat: "pql",
		query:       "select * from baseballStats limit 10",
	}))
}
