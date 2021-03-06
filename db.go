package bezsql

import (
	"context"
	"database/sql"
)

type DB interface {
	connect(databaseName string, config Config) (bool, error)
	DoesTableExist(table string) (bool, error)
	DoesColumnExist(table string, field string) (bool, error)
	SetParamPrefix(prefix string)
	RunParallel()
	GetConfig() Config
	NewQuery() (DB, error)
	Clone() (DB, error)
	Table(table string)
	TableSub(subDb DB, table string)
	RawQuery(query string, params []interface{}) (*sql.Rows, context.CancelFunc, error)
	RawNonQuery(query string, params []interface{}) (sql.Result, error)
	Insert(values map[string]interface{}, escape bool)
	InsertMulti(columns []string, rows [][]interface{}, escape bool)
	Update(values map[string]interface{}, escape bool)
	Cols(cols []string)
	Count(col string, alias string) string
	Sum(col string, alias string) string
	Avg(col string, alias string) string
	Max(col string, alias string) string
	Min(col string, alias string) string
	getParams() []interface{}
	getParamNames() []string
	GenerateSelect() string
	GenerateInsert() string
	GenerateUpdate() string
	JoinTable(tableName string, primaryKey string, foreignKey string)
	LeftJoinTable(tableName string, primaryKey string, foreignKey string)
	JoinSub(subSql DB, alias string, primaryKey string, foreignKey string)
	LeftJoinSub(subSql DB, alias string, primaryKey string, foreignKey string)
	JoinTableQuery(tableName string, queryFunc queryFunc)
	LeftJoinTableQuery(tableName string, queryFunc queryFunc)
	JoinSubQuery(subSql DB, alias string, queryFunc queryFunc)
	LeftJoinSubQuery(subSql DB, alias string, queryFunc queryFunc)
	Where(field string, comparator string, value interface{}, escape bool)
	WhereNull(field string)
	WhereNotNull(field string)
	WhereInList(field string, values []interface{}, escape bool)
	WhereNotInList(field string, values []interface{}, escape bool)
	WhereInSub(field string, subSql DB)
	WhereNotInSub(field string, subSql DB)
	Or()
	And()
	OpenBracket()
	CloseBracket()
	LimitBy(number int)
	OffsetBy(number int)
	OrderBy(field string, direction string)
	GroupBy(field ...string)
	Save() (sql.Result, error)
	Delete() (sql.Result, error)
	Fetch() (*sql.Rows, context.CancelFunc, error)
	FetchConcurrent() (successChannel chan bool, startRowsChannel chan bool, rowChannel chan *sql.Rows, nextChannel chan bool, completeChannel chan bool, cancelChannel chan bool, errorChannel chan error)
}
