package bezsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

var connections map[string]*sql.DB = make(map[string]*sql.DB)

type join struct {
	Type   string
	Table  string
	Query  Query
	Params []interface{}
}

type MySQL struct {
	databaseName string
	usedConfig   Config
	table        string
	cols         []string
	query        Query
	joins        []join
	params       []interface{}
}

func (db *MySQL) DoesTableExist(table string) bool {
	newDb := db.Clone()
	config := db.GetConfig()
	newDb.Table("information_schema.TABLES")
	newDb.Cols([]string{
		"COUNT(*) num",
	})
	newDb.Where("TABLE_SCHEMA", "=", config.Database, true)
	newDb.Where("TABLE_NAME", "=", table, true)
	res, close := newDb.Fetch()
	defer close()
	for res.Next() {
		var num int32
		res.Scan(&num)
		if num > 0 {
			return true
		}
	}
	return false
}

func (db *MySQL) DoesColumnExist(table string, field string) bool {
	newDb := db.Clone()
	config := db.GetConfig()
	newDb.Table("information_schema.COLUMNS")
	newDb.Cols([]string{
		"COUNT(*) num",
	})
	newDb.Where("TABLE_SCHEMA", "=", config.Database, true)
	newDb.Where("TABLE_NAME", "=", table, true)
	newDb.Where("COLUMN_NAME", "=", field, true)
	res, close := newDb.Fetch()
	defer close()
	for res.Next() {
		var num int32
		res.Scan(&num)
		if num > 0 {
			return true
		}
	}
	return false
}

func (db *MySQL) GetConfig() Config {
	return db.usedConfig
}

func (db *MySQL) RawQuery(query string, params []interface{}) (*sql.Rows, context.CancelFunc) {
	db.params = params
	return db.executeQuery(query)
}

func (db *MySQL) RawNonQuery(query string, params []interface{}) (sql.Result, context.CancelFunc) {
	db.params = params
	return db.executeNonQuery(query)
}

func (db *MySQL) Table(table string) {
	db.table = table
}

func (db *MySQL) GetParams() []interface{} {
	return db.params
}

func checkReserved(word string) string {
	reservedWords := []string{
		"select",
		"insert",
		"delete",
		"update",
		"where",
		"table",
		"join",
		"order",
		"read",
		"check"}
	if strings.Contains(word, ".") {
		wordParts := strings.Split(word, ".")
		escapedParts := []string{}
		for _, wordPart := range wordParts {
			for _, reservedWord := range reservedWords {
				if reservedWord == wordPart {
					wordPart = fmt.Sprintf("`%s`", wordPart)
					break
				}
			}
			escapedParts = append(escapedParts, wordPart)
		}
		return strings.Join(escapedParts, ".")
	} else {
		for _, reservedWord := range reservedWords {
			if reservedWord == word {
				word = fmt.Sprintf("`%s`", word)
				break
			}
		}
		return word
	}
}

func (db *MySQL) Cols(cols []string) {
	escapedCols := []string{}
	for _, col := range cols {
		escapedCols = append(escapedCols, checkReserved(col))
	}
	db.cols = escapedCols
}

func (db *MySQL) Clone() DB {
	newDB := MySQL{}
	newDB.Connect(db.databaseName, db.usedConfig)
	return &newDB
}

func (db *MySQL) Connect(databaseName string, config Config) bool {
	db.databaseName = databaseName
	db.usedConfig = config
	if _, exists := connections[databaseName]; !exists {
		mySQLConfig := mysql.NewConfig()
		mySQLConfig.User = config.Username
		mySQLConfig.Passwd = config.Password
		mySQLConfig.DBName = config.Database
		mySQLConfig.Addr = fmt.Sprintf("%s:%d", config.Host, config.Port)
		dbCon, err := sql.Open("mysql", mySQLConfig.FormatDSN())
		if err != nil {
			return false
		}
		connections[databaseName] = dbCon
	}

	/*
		results, err := dbCon.Query("SELECT * FROM users")
		if err != nil {

		}
		for results.Next() {
			var ue models.UserEntity
			err = results.Scan(&ue.Id, &ue.Username, &ue.Password, &ue.Email)
			if err != nil {

			}
			fmt.Println(ue)
		} */
	return false
}

func (db *MySQL) addTableJoin(joinType string, tableName string, primaryKey string, foreignKey string) {
	q := Query{}
	q.On(primaryKey, "=", foreignKey, false)
	db.joins = append(db.joins, join{
		Type:  joinType,
		Table: tableName,
		Query: q})
}

func (db *MySQL) JoinTable(tableName string, primaryKey string, foreignKey string) {
	db.addTableJoin("JOIN", tableName, primaryKey, foreignKey)
}

func (db *MySQL) LeftJoinTable(tableName string, primaryKey string, foreignKey string) {
	db.addTableJoin("LEFT JOIN", tableName, primaryKey, foreignKey)
}

func (db *MySQL) addSubJoin(joinType string, subSql DB, alias string, primaryKey string, foreignKey string) {
	q := Query{}
	q.On(primaryKey, "=", foreignKey, false)
	tableName := fmt.Sprintf("(%s) %s", subSql.GenerateSelect(), alias)
	params := subSql.GetParams()
	db.joins = append(db.joins, join{
		Type:   joinType,
		Table:  tableName,
		Query:  q,
		Params: params})
}

func (db *MySQL) JoinSub(subSql DB, alias string, primaryKey string, foreignKey string) {
	db.addSubJoin("JOIN", subSql, alias, primaryKey, foreignKey)
}

func (db *MySQL) LeftJoinSub(subSql DB, alias string, primaryKey string, foreignKey string) {
	db.addSubJoin("LEFT JOIN", subSql, alias, primaryKey, foreignKey)
}

type QueryFunc func(*Query)

func (db *MySQL) addQueryTableJoin(joinType string, tableName string, queryFunc QueryFunc) {
	q := Query{}
	queryFunc(&q)
	db.joins = append(db.joins, join{
		Type:  joinType,
		Table: tableName,
		Query: q})
}

func (db *MySQL) JoinTableQuery(tableName string, queryFunc QueryFunc) {
	db.addQueryTableJoin("JOIN", tableName, queryFunc)
}

func (db *MySQL) LeftJoinTableQuery(tableName string, queryFunc QueryFunc) {
	db.addQueryTableJoin("LEFT JOIN", tableName, queryFunc)
}

func (db *MySQL) addQuerySubJoin(joinType string, subSql DB, alias string, queryFunc QueryFunc) {
	q := Query{}
	queryFunc(&q)
	tableName := fmt.Sprintf("(%s) %s", subSql.GenerateSelect(), alias)
	params := subSql.GetParams()
	db.joins = append(db.joins, join{
		Type:   joinType,
		Table:  tableName,
		Query:  q,
		Params: params})
}

func (db *MySQL) JoinSubQuery(subSql DB, alias string, queryFunc QueryFunc) {
	db.addQuerySubJoin("JOIN", subSql, alias, queryFunc)
}

func (db *MySQL) LeftJoinSubQuery(subSql DB, alias string, queryFunc QueryFunc) {
	db.addQuerySubJoin("LEFT JOIN", subSql, alias, queryFunc)
}

func (db *MySQL) Where(field string, comparator string, value interface{}, escape bool) {
	switch v := value.(type) {
	case string:
		if !escape {
			value = checkReserved(v)
		}
	}

	db.query.Where(field, comparator, value, escape)
}

func (db *MySQL) addWhereInList(inType string, field string, values []interface{}, escape bool) {
	if !escape {
		var escapedValues []interface{}
		for _, value := range values {
			switch v := value.(type) {
			case string:
				escapedValues = append(escapedValues, checkReserved(v))
			default:
				escapedValues = append(escapedValues, value)
			}
		}
		values = escapedValues
	}
	if inType == "in" {
		db.query.WhereInList(field, values, escape)
	} else {
		db.query.WhereNotInList(field, values, escape)
	}

}

func (db *MySQL) WhereInList(field string, values []interface{}, escape bool) {
	db.addWhereInList("in", field, values, escape)
}

func (db *MySQL) WhereNotInList(field string, values []interface{}, escape bool) {
	db.addWhereInList("not in", field, values, escape)
}

func (db *MySQL) WhereInSub(field string, subSql DB) {
	db.query.WhereInSub(field, subSql)
}

func (db *MySQL) WhereNotInSub(field string, subSql DB) {
	db.query.WhereNotInSub(field, subSql)
}

func (db *MySQL) Or() {
	db.query.Or()
}
func (db *MySQL) And() {
	db.query.And()
}

func (db *MySQL) GenerateSelect() string {
	var params []interface{}
	query := "SELECT "
	query += strings.Join(db.cols, ",")
	query += " FROM "
	query += fmt.Sprintf(" %s ", db.table)

	for _, j := range db.joins {
		params = append(params, j.Params...)
		query += fmt.Sprintf(" %s %s ON ", j.Type, j.Table)
		whereString, jParams := j.Query.ApplyWheres()
		query += fmt.Sprintf(" %s ", whereString)
		params = append(params, jParams...)
	}

	if len(db.query.wheres) > 0 {
		whereString, newParams := db.query.ApplyWheres()
		params = newParams
		query += " WHERE " + whereString
	}
	db.params = params
	return query
}

func (db *MySQL) Fetch() (*sql.Rows, context.CancelFunc) {
	return db.executeQuery(db.GenerateSelect())
}

func (db *MySQL) executeQuery(query string) (*sql.Rows, context.CancelFunc) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	con := connections[db.databaseName]
	results, err := con.QueryContext(ctx, query, db.params...)

	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	return results, cancelFunc
}

func (db *MySQL) executeNonQuery(query string) (sql.Result, context.CancelFunc) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	con := connections[db.databaseName]
	results, err := con.ExecContext(ctx, query, db.params...)

	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	return results, cancelFunc
}
