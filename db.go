package bezsql

import (
	"context"
	"database/sql"
)

type DB interface {
	Connect(databaseName string, config Config) (bool, error)
	DoesTableExist(table string) (bool, error)
	DoesColumnExist(table string, field string) (bool, error)
	GetConfig() Config
	Clone() (DB, error)
	Table(table string)
	RawQuery(query string, params []interface{}) (*sql.Rows, context.CancelFunc, error)
	RawNonQuery(query string, params []interface{}) (sql.Result, context.CancelFunc, error)
	Insert(values map[string]interface{}, escape bool)
	Update(values map[string]interface{}, escape bool)
	Cols(cols []string)
	GetParams() []interface{}
	GenerateSelect() string
	GenerateInsert() string
	GenerateUpdate() string
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
	Save() (sql.Result, context.CancelFunc, error)
	Fetch() (*sql.Rows, context.CancelFunc, error)
}
