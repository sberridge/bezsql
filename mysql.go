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

type orderBy struct {
	Field     string
	Direction string
}

type MySQL struct {
	databaseName      string
	usedConfig        Config
	table             string
	cols              []string
	query             Query
	joins             []join
	params            []interface{}
	insertValues      []string
	multiInsertValues [][]string
	insertColumns     []string
	updateValues      []string
	limitBy           int
	offsetBy          int
	ordering          []orderBy
	groupColumns      []string
	parallel          bool
}

func (db *MySQL) DoesTableExist(table string) (bool, error) {
	newDb, err := db.NewQuery()
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

func (db *MySQL) RunParallel() {
	db.parallel = true
}

func (db *MySQL) DoesColumnExist(table string, field string) (bool, error) {
	newDb, err := db.NewQuery()
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

func (db *MySQL) RawNonQuery(query string, params []interface{}) (sql.Result, error) {
	db.params = params
	return db.executeNonQuery(query)
}

func (db *MySQL) Table(table string) {
	db.table = table
}

func (db *MySQL) GetParams() []interface{} {
	return db.params
}

func (db *MySQL) checkReserved(word string) string {
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
		escapedCols = append(escapedCols, db.checkReserved(col))
	}
	db.cols = escapedCols
}

func (db *MySQL) Count(col string, alias string) string {
	return fmt.Sprintf("COUNT(%s) %s", db.checkReserved(col), db.checkReserved(alias))
}

func (db *MySQL) NewQuery() (DB, error) {
	newDB := MySQL{}
	_, err := newDB.Connect(db.databaseName, db.usedConfig)
	if err != nil {
		return nil, err
	}
	return &newDB, nil
}

func (db *MySQL) Clone() (DB, error) {
	newDB := MySQL{}
	_, err := newDB.Connect(db.databaseName, db.usedConfig)
	if err != nil {
		return nil, err
	}

	newDB.table = db.table
	newDB.cols = db.cols
	newDB.groupColumns = db.groupColumns
	newDB.insertColumns = db.insertColumns
	newDB.insertValues = db.insertValues
	newDB.updateValues = db.updateValues
	newDB.joins = db.joins
	newDB.multiInsertValues = db.multiInsertValues
	newDB.ordering = db.ordering
	newDB.params = db.params
	newDB.query = db.query
	newDB.parallel = db.parallel

	return &newDB, err

}

func (db *MySQL) openConnection() (*sql.DB, error) {
	mySQLConfig := mysql.NewConfig()
	mySQLConfig.User = db.usedConfig.Username
	mySQLConfig.Passwd = db.usedConfig.Password
	mySQLConfig.DBName = db.usedConfig.Database
	mySQLConfig.Addr = fmt.Sprintf("%s:%d", db.usedConfig.Host, db.usedConfig.Port)
	odb, err := sql.Open("mysql", mySQLConfig.FormatDSN())
	odb.SetMaxIdleConns(0)
	odb.SetMaxOpenConns(5000)
	return odb, err
}

func (db *MySQL) Connect(databaseName string, config Config) (bool, error) {
	db.databaseName = databaseName
	db.usedConfig = config
	if _, exists := connections[databaseName]; !exists {

		dbCon, err := db.openConnection()
		if err != nil {
			return false, err
		}
		connections[databaseName] = dbCon
	}

	return true, nil
}

func (db *MySQL) Insert(values map[string]interface{}, escape bool) {
	var params []interface{}
	insertColumns := []string{}
	insertValues := []string{}
	for key, val := range values {
		insertColumns = append(insertColumns, db.checkReserved(key))
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
	db.params = params
	db.insertColumns = insertColumns
	db.insertValues = insertValues
}

func (db *MySQL) InsertMulti(columns []string, rows [][]interface{}, escape bool) {
	var params []interface{}
	insertColumns := []string{}
	multiInsertValues := [][]string{}
	for _, col := range columns {
		insertColumns = append(insertColumns, db.checkReserved(col))
	}
	for _, row := range rows {
		rowInsertValues := []string{}
		for _, val := range row {
			if escape {
				params = append(params, val)
				rowInsertValues = append(rowInsertValues, "?")
			} else {
				switch v := val.(type) {
				case string:
					rowInsertValues = append(rowInsertValues, v)
				}
			}
		}
		multiInsertValues = append(multiInsertValues, rowInsertValues)
	}
	db.insertColumns = insertColumns
	db.params = params
	db.multiInsertValues = multiInsertValues
}

func (db *MySQL) Update(values map[string]interface{}, escape bool) {
	var params []interface{}
	updateStrings := []string{}

	for key, val := range values {
		if escape {
			params = append(params, val)
			updateStrings = append(updateStrings, fmt.Sprintf("%s = %s", db.checkReserved(key), "?"))
		} else {
			switch v := val.(type) {
			case string:
				updateStrings = append(updateStrings, fmt.Sprintf("%s = %s", db.checkReserved(key), v))
			}
		}
	}
	db.params = params
	db.updateValues = updateStrings
}

func (db *MySQL) addTableJoin(joinType string, tableName string, primaryKey string, foreignKey string) {
	q := Query{}
	q.On(db.checkReserved(primaryKey), "=", db.checkReserved(foreignKey), false)
	db.joins = append(db.joins, join{
		Type:  joinType,
		Table: db.checkReserved(tableName),
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
	q.On(db.checkReserved(primaryKey), "=", db.checkReserved(foreignKey), false)
	tableName := fmt.Sprintf("(%s) %s", subSql.GenerateSelect(), db.checkReserved(alias))
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
		Table: db.checkReserved(tableName),
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
	tableName := fmt.Sprintf("(%s) %s", subSql.GenerateSelect(), db.checkReserved(alias))
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
	db.query.Where(db.checkReserved(field), comparator, value, escape)
}

func (db *MySQL) WhereNull(field string) {
	db.query.WhereNull(db.checkReserved(field))
}

func (db *MySQL) WhereNotNull(field string) {
	db.query.WhereNotNull(db.checkReserved(field))
}

func (db *MySQL) addWhereInList(inType string, field string, values []interface{}, escape bool) {
	if inType == "in" {
		db.query.WhereInList(db.checkReserved(field), values, escape)
	} else {
		db.query.WhereNotInList(db.checkReserved(field), values, escape)
	}
}

func (db *MySQL) WhereInList(field string, values []interface{}, escape bool) {
	db.addWhereInList("in", db.checkReserved(field), values, escape)
}

func (db *MySQL) WhereNotInList(field string, values []interface{}, escape bool) {
	db.addWhereInList("not in", db.checkReserved(field), values, escape)
}

func (db *MySQL) WhereInSub(field string, subSql DB) {
	db.query.WhereInSub(db.checkReserved(field), subSql)
}

func (db *MySQL) WhereNotInSub(field string, subSql DB) {
	db.query.WhereNotInSub(db.checkReserved(field), subSql)
}

func (db *MySQL) Or() {
	db.query.Or()
}
func (db *MySQL) And() {
	db.query.And()
}

func (db *MySQL) OpenBracket() {
	db.query.OpenBracket()
}

func (db *MySQL) CloseBracket() {
	db.query.CloseBracket()
}

func (db *MySQL) LimitBy(number int) {
	db.limitBy = number
}

func (db *MySQL) OffsetBy(number int) {
	db.offsetBy = number
}

func (db *MySQL) OrderBy(field string, direction string) {
	direction = strings.ToUpper(direction)
	if direction == "ASC" || direction == "DESC" {
		db.ordering = append(db.ordering, orderBy{
			Field:     db.checkReserved(field),
			Direction: direction,
		})
	}

}

func (db *MySQL) GroupBy(field ...string) {
	for _, f := range field {
		db.groupColumns = append(db.groupColumns, db.checkReserved(f))
	}

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

	if len(db.groupColumns) > 0 {
		query += fmt.Sprintf(" GROUP BY %s", strings.Join(db.groupColumns, ","))

	}

	if len(db.ordering) > 0 {
		query += " ORDER BY "
		orderStrings := []string{}
		for _, o := range db.ordering {
			orderStrings = append(orderStrings, fmt.Sprintf("%s %s", o.Field, o.Direction))
		}
		query += strings.Join(orderStrings, ", ")
	}

	if db.limitBy > 0 {
		query += fmt.Sprintf(" LIMIT %d ", db.limitBy)
		if db.offsetBy > 0 {
			query += fmt.Sprintf(" OFFSET %d ", db.offsetBy)
		}
	}
	fmt.Println(query)
	db.params = params
	return query
}
func (db *MySQL) GenerateInsert() string {
	query := fmt.Sprintf("INSERT INTO %s ", db.table)
	query += fmt.Sprintf(" (%s) VALUES ", strings.Join(db.insertColumns, ","))
	if len(db.insertValues) > 0 {
		query += fmt.Sprintf(" (%s) ", strings.Join(db.insertValues, ","))
	} else if len(db.multiInsertValues) > 0 {
		insertRows := []string{}
		for _, row := range db.multiInsertValues {
			insertRows = append(insertRows, fmt.Sprintf(" (%s) ", strings.Join(row, ",")))
		}
		query += strings.Join(insertRows, ",")
	}
	return query
}
func (db *MySQL) GenerateUpdate() string {
	query := fmt.Sprintf("UPDATE %s SET ", db.table)
	query += strings.Join(db.updateValues, ",")
	if len(db.query.wheres) > 0 {
		whereStr, newParams := db.query.ApplyWheres()
		query += fmt.Sprintf(" WHERE %s ", whereStr)
		db.params = append(db.params, newParams...)
	}
	return query
}

func (db *MySQL) GenerateDelete() string {
	query := fmt.Sprintf("DELETE FROM %s ", db.table)
	if len(db.query.wheres) > 0 {
		whereStr, newParams := db.query.ApplyWheres()
		query += fmt.Sprintf(" WHERE %s ", whereStr)
		db.params = append(db.params, newParams...)
	}
	return query
}

func (db *MySQL) Save() (sql.Result, error) {
	var query string
	if len(db.insertValues) > 0 || len(db.multiInsertValues) > 0 {
		query = db.GenerateInsert()
	} else if len(db.updateValues) > 0 {
		query = db.GenerateUpdate()
	}
	return db.executeNonQuery(query)
}
func (db *MySQL) Fetch() (*sql.Rows, context.CancelFunc, error) {

	return db.executeQuery(db.GenerateSelect())
}

func (db *MySQL) FetchConcurrent() (successChannel chan bool, startRowsChannel chan bool, rowChannel chan *sql.Rows, nextChannel chan bool, completeChannel chan bool, cancelChannel chan bool, errorChannel chan error) {
	successChannel = make(chan bool)
	startRowsChannel = make(chan bool)
	rowChannel = make(chan *sql.Rows)
	nextChannel = make(chan bool)
	completeChannel = make(chan bool)
	cancelChannel = make(chan bool)
	errorChannel = make(chan error)
	go db.concExecuteQuery(db.GenerateSelect(), successChannel, startRowsChannel, rowChannel, nextChannel, completeChannel, cancelChannel, errorChannel)
	return successChannel, startRowsChannel, rowChannel, nextChannel, completeChannel, cancelChannel, errorChannel
}

func (db *MySQL) Delete() (sql.Result, error) {
	return db.executeNonQuery(db.GenerateDelete())
}

func (db *MySQL) concExecuteQuery(query string, successChannel chan bool, startRowsChannel chan bool, rowChan chan *sql.Rows, nextChan chan bool, completeChan chan bool, cancelChan chan bool, errorChan chan error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelFunc()
	con := connections[db.databaseName]

	if db.parallel {
		newCon, err := db.openConnection()
		if err != nil {
			fmt.Println(err)
			errorChan <- err
		}
		con = newCon
		defer con.Close()
	}
	results, err := con.QueryContext(ctx, query, db.params...)

	if err != nil {
		fmt.Println(err)
		errorChan <- err
	}
	successChannel <- true
	cancelled := false
	select {
	case <-startRowsChannel:
		for results.Next() {
			select {
			case rowChan <- results:
				<-nextChan
			case <-cancelChan:
				cancelled = true
			}
			if cancelled {
				return
			}
		}
	case <-cancelChan:
		results.Close()
		return
	}

	completeChan <- true
}

func (db *MySQL) executeQuery(query string) (*sql.Rows, context.CancelFunc, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 60*time.Second)
	con := connections[db.databaseName]

	if db.parallel {
		newCon, err := db.openConnection()
		if err != nil {
			fmt.Println(err)
			defer cancelFunc()
			return nil, nil, err
		}
		con = newCon
		defer con.Close()
	}
	results, err := con.QueryContext(ctx, query, db.params...)

	if err != nil {
		fmt.Println(err)
		defer cancelFunc()
		return nil, nil, err
	}
	return results, cancelFunc, nil
}

func (db *MySQL) executeNonQuery(query string) (sql.Result, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelFunc()
	con := connections[db.databaseName]

	if db.parallel {
		newCon, err := db.openConnection()
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		con = newCon
		defer con.Close()
	}

	results, err := con.ExecContext(ctx, query, db.params...)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return results, nil
}
