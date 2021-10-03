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
	databaseName  string
	usedConfig    Config
	table         string
	cols          []string
	query         Query
	joins         []join
	params        []interface{}
	insertValues  []string
	insertColumns []string
	updateValues  []string
}

func (db *MySQL) DoesTableExist(table string) (bool, error) {
	newDb, err := db.Clone()
	if err != nil {
		return false, err
	}
	config := db.GetConfig()
	newDb.Table("information_schema.TABLES")
	newDb.Cols([]string{
		"COUNT(*) num",
	})
	newDb.Where("TABLE_SCHEMA", "=", config.Database, true)
	newDb.Where("TABLE_NAME", "=", table, true)
	res, close, err := newDb.Fetch()
	if err != nil {
		return false, err
	}
	defer close()
	for res.Next() {
		var num int32
		res.Scan(&num)
		if num > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (db *MySQL) DoesColumnExist(table string, field string) (bool, error) {
	newDb, err := db.Clone()
	if err != nil {
		return false, err
	}
	config := db.GetConfig()
	newDb.Table("information_schema.COLUMNS")
	newDb.Cols([]string{
		"COUNT(*) num",
	})
	newDb.Where("TABLE_SCHEMA", "=", config.Database, true)
	newDb.Where("TABLE_NAME", "=", table, true)
	newDb.Where("COLUMN_NAME", "=", field, true)
	res, close, err := newDb.Fetch()
	if err != nil {
		return false, err
	}
	defer close()
	for res.Next() {
		var num int32
		res.Scan(&num)
		if num > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (db *MySQL) GetConfig() Config {
	return db.usedConfig
}

func (db *MySQL) RawQuery(query string, params []interface{}) (*sql.Rows, context.CancelFunc, error) {
	db.params = params
	return db.executeQuery(query)
}

func (db *MySQL) RawNonQuery(query string, params []interface{}) (sql.Result, context.CancelFunc, error) {
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

func (db *MySQL) Clone() (DB, error) {
	newDB := MySQL{}
	_, err := newDB.Connect(db.databaseName, db.usedConfig)
	if err != nil {
		return nil, err
	}
	return &newDB, nil
}

func (db *MySQL) Connect(databaseName string, config Config) (bool, error) {
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
			return false, err
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
	return true, nil
}

func (db *MySQL) Insert(values map[string]interface{}, escape bool) {
	var params []interface{}
	insertColumns := []string{}
	insertValues := []string{}
	if escape {
		for key, val := range values {
			insertColumns = append(insertColumns, checkReserved(key))
			if escape {
				params = append(params, val)
				insertValues = append(insertValues, "?")
			} else {
				switch v := val.(type) {
				case string:
					insertValues = append(insertValues, v)
				}
			}

		}
	}
	db.params = params
	db.insertColumns = insertColumns
	db.insertValues = insertValues
}

func (db *MySQL) Update(values map[string]interface{}, escape bool) {
	var params []interface{}
	updateStrings := []string{}

	for key, val := range values {
		if escape {
			params = append(params, val)
			updateStrings = append(updateStrings, fmt.Sprintf("%s = %s", checkReserved(key), "?"))
		} else {
			switch v := val.(type) {
			case string:
				updateStrings = append(updateStrings, fmt.Sprintf("%s = %s", checkReserved(key), v))
			}
		}
	}
	db.params = params
	db.updateValues = updateStrings
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
func (db *MySQL) GenerateInsert() string {
	query := fmt.Sprintf("INSERT INTO %s ", db.table)
	query += fmt.Sprintf(" (%s) VALUES(%s) ", strings.Join(db.insertColumns, ","), strings.Join(db.insertValues, ","))
	return query
}
func (db *MySQL) GenerateUpdate() string {
	query := fmt.Sprintf("UPDATE TABLE %s SET ", db.table)
	query += strings.Join(db.updateValues, ",")
	if len(db.query.wheres) > 0 {
		whereStr, newParams := db.query.ApplyWheres()
		query += fmt.Sprintf(" WHERE %s ", whereStr)
		db.params = append(db.params, newParams...)
	}
	return query
}
func (db *MySQL) Save() (sql.Result, context.CancelFunc, error) {
	var query string
	if len(db.insertValues) > 0 {
		query = db.GenerateInsert()
	} else if len(db.updateValues) > 0 {
		query = db.GenerateUpdate()
	}
	return db.executeNonQuery(query)
}
func (db *MySQL) Fetch() (*sql.Rows, context.CancelFunc, error) {
	return db.executeQuery(db.GenerateSelect())
}

func (db *MySQL) executeQuery(query string) (*sql.Rows, context.CancelFunc, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	con := connections[db.databaseName]
	results, err := con.QueryContext(ctx, query, db.params...)

	if err != nil {
		fmt.Println(err)
		defer cancelFunc()
		return nil, nil, err
	}
	return results, cancelFunc, nil
}

func (db *MySQL) executeNonQuery(query string) (sql.Result, context.CancelFunc, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	con := connections[db.databaseName]
	results, err := con.ExecContext(ctx, query, db.params...)

	if err != nil {
		fmt.Println(err)
		defer cancelFunc()
		return nil, nil, err
	}
	return results, cancelFunc, nil
}
