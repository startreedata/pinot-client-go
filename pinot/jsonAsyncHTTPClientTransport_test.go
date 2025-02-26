package pinot

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
	assert.True(t, strings.HasPrefix(err.Error(), "parse "))

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
	assert.True(t, strings.HasPrefix(err.Error(), "Post "))
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
		client: http.DefaultClient,
		header: map[string]string{"a": "b"},
	}

	// should not have timeoutMs
	assert.Equal(t, "", transport.buildQueryOptions(&Request{
		queryFormat: "pql",
		query:       "select * from baseballStats limit 10",
	}))
}
