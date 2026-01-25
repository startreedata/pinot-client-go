package gormpinot

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/startreedata/pinot-client-go/pinot"
)

func TestDialectorNameAndExplain(t *testing.T) {
	d := Dialector{}
	require.Equal(t, "pinot", d.Name())

	explained := d.Explain("select * from foo where id = ?", 10)
	require.Contains(t, explained, "10")
}

func TestDialectorQuoteTo(t *testing.T) {
	var buf bytes.Buffer
	d := Dialector{}
	d.QuoteTo(&buf, "my_table.my_column")
	require.Equal(t, `"my_table"."my_column"`, buf.String())
}

func TestDialectorQuoteToEscapesQuotes(t *testing.T) {
	var buf bytes.Buffer
	d := Dialector{}
	d.QuoteTo(&buf, `weird"name`)
	require.Equal(t, `"weird""name"`, buf.String())
}

func TestDialectorQuoteToSelfQuoted(t *testing.T) {
	var buf bytes.Buffer
	d := Dialector{}
	d.QuoteTo(&buf, `"quoted"`)
	require.Equal(t, `"quoted"`, buf.String())
}

func TestDialectorQuoteToDoubleQuotes(t *testing.T) {
	var buf bytes.Buffer
	d := Dialector{}
	d.QuoteTo(&buf, `weird""name`)
	require.Equal(t, `"weird""name"`, buf.String())
}

func TestDialectorBindVarTo(t *testing.T) {
	var buf bytes.Buffer
	d := Dialector{}
	d.BindVarTo(&buf, nil, nil)
	require.Equal(t, "?", buf.String())
}

func TestDialectorInitializeRequiresConn(t *testing.T) {
	_, err := gorm.Open(Open(Config{}), &gorm.Config{})
	require.Error(t, err)
}

func TestDialectorInitializeSuccess(t *testing.T) {
	_, err := gorm.Open(Open(Config{Conn: &pinot.Connection{}}), &gorm.Config{})
	require.NoError(t, err)
}

func TestDialectorDefaultValueOf(t *testing.T) {
	d := Dialector{}
	expr := d.DefaultValueOf(nil)
	require.Equal(t, clause.Expr{SQL: "DEFAULT"}, expr)
}

func TestDialectorDataTypeOf(t *testing.T) {
	d := Dialector{}
	require.Equal(t, "", d.DataTypeOf(nil))
}

func TestDialectorMigrator(t *testing.T) {
	d := Dialector{}
	m := d.Migrator(&gorm.DB{})
	_, ok := m.(unsupportedMigrator)
	require.True(t, ok)
}
