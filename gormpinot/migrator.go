package gormpinot

import (
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

var errUnsupportedMigration = errors.New("pinot migrator is not supported")

type unsupportedMigrator struct {
	db *gorm.DB
}

func (m unsupportedMigrator) AutoMigrate(dst ...interface{}) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) CurrentDatabase() string {
	return ""
}

func (m unsupportedMigrator) FullDataTypeOf(*schema.Field) clause.Expr {
	return clause.Expr{}
}

func (m unsupportedMigrator) GetTypeAliases(string) []string {
	return nil
}

func (m unsupportedMigrator) CreateTable(dst ...interface{}) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) DropTable(dst ...interface{}) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) HasTable(dst interface{}) bool {
	return false
}

func (m unsupportedMigrator) RenameTable(oldName, newName interface{}) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) GetTables() ([]string, error) {
	return nil, errUnsupportedMigration
}

func (m unsupportedMigrator) TableType(dst interface{}) (gorm.TableType, error) {
	return nil, errUnsupportedMigration
}

func (m unsupportedMigrator) AddColumn(dst interface{}, field string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) DropColumn(dst interface{}, field string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) AlterColumn(dst interface{}, field string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) MigrateColumn(dst interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) MigrateColumnUnique(dst interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) HasColumn(dst interface{}, field string) bool {
	return false
}

func (m unsupportedMigrator) RenameColumn(dst interface{}, oldName, field string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) ColumnTypes(dst interface{}) ([]gorm.ColumnType, error) {
	return nil, errUnsupportedMigration
}

func (m unsupportedMigrator) CreateView(name string, option gorm.ViewOption) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) DropView(name string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) CreateConstraint(dst interface{}, name string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) DropConstraint(dst interface{}, name string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) HasConstraint(dst interface{}, name string) bool {
	return false
}

func (m unsupportedMigrator) CreateIndex(dst interface{}, name string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) DropIndex(dst interface{}, name string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) HasIndex(dst interface{}, name string) bool {
	return false
}

func (m unsupportedMigrator) RenameIndex(dst interface{}, oldName, newName string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) GetIndexes(dst interface{}) ([]gorm.Index, error) {
	return nil, errUnsupportedMigration
}
