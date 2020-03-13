package pinot

import (
	"testing"

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
