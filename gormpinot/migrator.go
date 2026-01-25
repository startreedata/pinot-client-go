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

func (m unsupportedMigrator) AutoMigrate(_ ...interface{}) error {
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

func (m unsupportedMigrator) CreateTable(_ ...interface{}) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) DropTable(_ ...interface{}) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) HasTable(_ interface{}) bool {
	return false
}

func (m unsupportedMigrator) RenameTable(_, _ interface{}) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) GetTables() ([]string, error) {
	return nil, errUnsupportedMigration
}

func (m unsupportedMigrator) TableType(_ interface{}) (gorm.TableType, error) {
	return nil, errUnsupportedMigration
}

func (m unsupportedMigrator) AddColumn(_ interface{}, _ string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) DropColumn(_ interface{}, _ string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) AlterColumn(_ interface{}, _ string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) MigrateColumn(_ interface{}, _ *schema.Field, _ gorm.ColumnType) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) MigrateColumnUnique(_ interface{}, _ *schema.Field, _ gorm.ColumnType) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) HasColumn(_ interface{}, _ string) bool {
	return false
}

func (m unsupportedMigrator) RenameColumn(_ interface{}, _ string, _ string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) ColumnTypes(_ interface{}) ([]gorm.ColumnType, error) {
	return nil, errUnsupportedMigration
}

func (m unsupportedMigrator) CreateView(_ string, _ gorm.ViewOption) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) DropView(_ string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) CreateConstraint(_ interface{}, _ string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) DropConstraint(_ interface{}, _ string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) HasConstraint(_ interface{}, _ string) bool {
	return false
}

func (m unsupportedMigrator) CreateIndex(_ interface{}, _ string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) DropIndex(_ interface{}, _ string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) HasIndex(_ interface{}, _ string) bool {
	return false
}

func (m unsupportedMigrator) RenameIndex(_ interface{}, _ string, _ string) error {
	return errUnsupportedMigration
}

func (m unsupportedMigrator) GetIndexes(_ interface{}) ([]gorm.Index, error) {
	return nil, errUnsupportedMigration
}
