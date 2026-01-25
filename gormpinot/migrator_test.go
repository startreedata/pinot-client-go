package gormpinot

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestUnsupportedMigrator(t *testing.T) {
	m := unsupportedMigrator{}

	require.ErrorIs(t, m.AutoMigrate(), errUnsupportedMigration)
	require.Equal(t, "", m.CurrentDatabase())
	require.Equal(t, []string(nil), m.GetTypeAliases(""))
	require.Equal(t, clause.Expr{}, m.FullDataTypeOf(nil))
	require.Equal(t, false, m.HasTable(nil))
	require.Equal(t, false, m.HasColumn(nil, "col"))
	require.Equal(t, false, m.HasConstraint(nil, "constraint"))
	require.Equal(t, false, m.HasIndex(nil, "index"))

	require.ErrorIs(t, m.CreateTable(), errUnsupportedMigration)
	require.ErrorIs(t, m.DropTable(), errUnsupportedMigration)
	require.ErrorIs(t, m.RenameTable(nil, nil), errUnsupportedMigration)
	tables, err := m.GetTables()
	require.ErrorIs(t, err, errUnsupportedMigration)
	require.Nil(t, tables)
	_, err = m.TableType(nil)
	require.ErrorIs(t, err, errUnsupportedMigration)
	require.ErrorIs(t, m.AddColumn(nil, ""), errUnsupportedMigration)
	require.ErrorIs(t, m.DropColumn(nil, ""), errUnsupportedMigration)
	require.ErrorIs(t, m.AlterColumn(nil, ""), errUnsupportedMigration)
	require.ErrorIs(t, m.MigrateColumn(nil, nil, nil), errUnsupportedMigration)
	require.ErrorIs(t, m.MigrateColumnUnique(nil, nil, nil), errUnsupportedMigration)
	require.ErrorIs(t, m.RenameColumn(nil, "", ""), errUnsupportedMigration)
	_, err = m.ColumnTypes(nil)
	require.ErrorIs(t, err, errUnsupportedMigration)
	require.ErrorIs(t, m.CreateView("", gorm.ViewOption{}), errUnsupportedMigration)
	require.ErrorIs(t, m.DropView(""), errUnsupportedMigration)
	require.ErrorIs(t, m.CreateConstraint(nil, ""), errUnsupportedMigration)
	require.ErrorIs(t, m.DropConstraint(nil, ""), errUnsupportedMigration)
	require.ErrorIs(t, m.CreateIndex(nil, ""), errUnsupportedMigration)
	require.ErrorIs(t, m.DropIndex(nil, ""), errUnsupportedMigration)
	require.ErrorIs(t, m.RenameIndex(nil, "", ""), errUnsupportedMigration)
	_, err = m.GetIndexes(nil)
	require.ErrorIs(t, err, errUnsupportedMigration)
}
