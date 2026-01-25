// Package gormpinot provides a GORM dialector that executes Pinot SQL over HTTP.
//
// Limitations:
//   - Read-only: INSERT/UPDATE/DELETE/DDL are not supported.
//   - Migrations are not supported.
//   - Broker selection uses Config.DefaultTable when provided; otherwise a best-effort
//     table name is inferred from the SQL or an empty table name is used.
package gormpinot
