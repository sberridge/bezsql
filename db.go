package bezsql

import (
	"context"
	"database/sql"
)

type DB interface {
	Connect(databaseName string, config Config) bool
	DoesTableExist(table string) bool

	GetConfig() Config
	Clone() DB
	Table(table string)
	RawQuery(query string, params []interface{}) (*sql.Rows, context.CancelFunc)
	RawNonQuery(query string, params []interface{}) (sql.Result, context.CancelFunc)
	Cols(cols []string)
	GetParams() []interface{}
	GenerateSelect() string
	JoinTable(tableName string, primaryKey string, foreignKey string)
	LeftJoinTable(tableName string, primaryKey string, foreignKey string)
	JoinSub(subSql DB, alias string, primaryKey string, foreignKey string)
	LeftJoinSub(subSql DB, alias string, primaryKey string, foreignKey string)
	JoinTableQuery(tableName string, queryFunc QueryFunc)
	LeftJoinTableQuery(tableName string, queryFunc QueryFunc)
	JoinSubQuery(subSql DB, alias string, queryFunc QueryFunc)
	LeftJoinSubQuery(subSql DB, alias string, queryFunc QueryFunc)
	Where(field string, comparator string, value interface{}, escape bool)
	WhereInList(field string, values []interface{}, escape bool)
	WhereNotInList(field string, values []interface{}, escape bool)
	WhereInSub(field string, subSql DB)
	WhereNotInSub(field string, subSql DB)
	Or()
	And()
	Fetch() (*sql.Rows, context.CancelFunc)
}
