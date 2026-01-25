package gormpinot

import (
	"database/sql"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	"github.com/startreedata/pinot-client-go/pinot"
)

// Config configures the Pinot GORM dialector.
type Config struct {
	Conn         *pinot.Connection
	DefaultTable string
}

// Dialector is the GORM dialector for Pinot.
type Dialector struct {
	config Config
}

// Open returns a GORM dialector configured for Pinot.
func Open(config Config) gorm.Dialector {
	return Dialector{config: config}
}

// Name returns the dialector name.
func (Dialector) Name() string {
	return "pinot"
}

// Initialize wires the dialector into the GORM DB instance.
func (d Dialector) Initialize(db *gorm.DB) error {
	if d.config.Conn == nil {
		return errors.New("pinot connection is required")
	}
	db.DisableAutomaticPing = true

	connector := newConnector(d.config.Conn, d.config.DefaultTable)
	db.ConnPool = sql.OpenDB(connector)

	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses:        []string{"INSERT", "VALUES", "ON CONFLICT", "RETURNING"},
		UpdateClauses:        []string{"UPDATE", "SET", "WHERE", "RETURNING"},
		DeleteClauses:        []string{"DELETE", "FROM", "WHERE", "RETURNING"},
		LastInsertIDReversed: false,
	})
	return nil
}

// Migrator returns a migrator that rejects schema operations.
func (Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return unsupportedMigrator{db: db}
}

// DataTypeOf returns an empty datatype since migrations are unsupported.
func (Dialector) DataTypeOf(*schema.Field) string {
	return ""
}

// DefaultValueOf returns DEFAULT for compatibility.
func (Dialector) DefaultValueOf(*schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

// BindVarTo writes a placeholder.
func (Dialector) BindVarTo(writer clause.Writer, _ *gorm.Statement, _ interface{}) {
	writeByte(writer, '?')
}

// QuoteTo quotes identifiers with double quotes.
func (Dialector) QuoteTo(writer clause.Writer, str string) {
	var (
		underQuoted, selfQuoted bool
		continuousQuote         int8
		shiftDelimiter          int8
	)

	for _, v := range []byte(str) {
		switch v {
		case '"':
			continuousQuote++
			if continuousQuote == 2 {
				writeString(writer, `""`)
				continuousQuote = 0
			}
		case '.':
			if continuousQuote > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousQuote = 0
				writeByte(writer, '"')
			}
			writeByte(writer, v)
			continue
		default:
			if shiftDelimiter-continuousQuote <= 0 && !underQuoted {
				writeByte(writer, '"')
				underQuoted = true
				if selfQuoted = continuousQuote > 0; selfQuoted {
					continuousQuote--
				}
			}

			for ; continuousQuote > 0; continuousQuote-- {
				writeString(writer, `""`)
			}

			writeByte(writer, v)
		}
		shiftDelimiter++
	}

	if continuousQuote > 0 && !selfQuoted {
		writeString(writer, `""`)
	}
	writeByte(writer, '"')
}

func writeByte(writer clause.Writer, value byte) {
	//nolint:errcheck
	_ = writer.WriteByte(value)
}

func writeString(writer clause.Writer, value string) {
	//nolint:errcheck
	_, _ = writer.WriteString(value)
}

// Explain returns SQL with rendered parameters for logging.
func (Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, "'", vars...)
}
