package bezsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type mySQL struct {
	databaseName      string
	usedConfig        Config
	table             string
	cols              []string
	query             Query
	joins             []join
	params            []interface{}
	paramNames        []string
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

func (db *mySQL) SetParamPrefix(prefix string) {
	db.query.SetParamPrefix(prefix)
}

func (db *mySQL) DoesTableExist(table string) (bool, error) {
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

func (db *mySQL) RunParallel() {
	db.parallel = true
}

func (db *mySQL) DoesColumnExist(table string, field string) (bool, error) {
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

func (db *mySQL) GetConfig() Config {
	return db.usedConfig
}

func (db *mySQL) RawQuery(query string, params []interface{}) (*sql.Rows, context.CancelFunc, error) {
	db.params = params
	return db.executeQuery(query)
}

func (db *mySQL) RawNonQuery(query string, params []interface{}) (sql.Result, error) {
	db.params = params
	return db.executeNonQuery(query)
}

func (db *mySQL) Table(table string) {
	db.table = table
}

func (db *mySQL) TableSub(subDb DB, alias string) {
	db.table = fmt.Sprintf(" (%s) %s ", subDb.GenerateSelect(), db.checkReserved(alias))
	db.params = append(db.params, subDb.getParams()...)
	db.paramNames = append(db.paramNames, subDb.getParamNames()...)
}

func (db *mySQL) getParams() []interface{} {
	return db.params
}

func (db *mySQL) getParamNames() []string {
	return db.paramNames
}

func (db *mySQL) checkReserved(word string) string {
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

func (db *mySQL) Cols(cols []string) {
	escapedCols := []string{}
	for _, col := range cols {
		escapedCols = append(escapedCols, db.checkReserved(col))
	}
	db.cols = escapedCols
}

func (db *mySQL) Count(col string, alias string) string {
	return fmt.Sprintf("COUNT(%s) %s", db.checkReserved(col), db.checkReserved(alias))
}

func (db *mySQL) Sum(col string, alias string) string {
	return fmt.Sprintf("SUM(%s) %s", db.checkReserved(col), db.checkReserved(alias))
}

func (db *mySQL) Avg(col string, alias string) string {
	return fmt.Sprintf("AVG(%s) %s", db.checkReserved(col), db.checkReserved(alias))
}

func (db *mySQL) Max(col string, alias string) string {
	return fmt.Sprintf("MAX(%s) %s", db.checkReserved(col), db.checkReserved(alias))
}

func (db *mySQL) Min(col string, alias string) string {
	return fmt.Sprintf("MIN(%s) %s", db.checkReserved(col), db.checkReserved(alias))
}

func (db *mySQL) NewQuery() (DB, error) {
	newDB := mySQL{}
	_, err := newDB.connect(db.databaseName, db.usedConfig)
	if err != nil {
		return nil, err
	}
	return &newDB, nil
}

func (db *mySQL) Clone() (DB, error) {
	newDB := mySQL{}
	_, err := newDB.connect(db.databaseName, db.usedConfig)
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

func (db *mySQL) openConnection() (*sql.DB, error) {
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

func (db *mySQL) connect(databaseName string, config Config) (bool, error) {
	db.databaseName = databaseName
	db.usedConfig = config
	if _, exists := openConnections[databaseName]; !exists {

		dbCon, err := db.openConnection()
		if err != nil {
			return false, err
		}
		openConnections[databaseName] = dbCon
	}

	return true, nil
}

func (db *mySQL) Insert(values map[string]interface{}, escape bool) {
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

func (db *mySQL) InsertMulti(columns []string, rows [][]interface{}, escape bool) {
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

func (db *mySQL) Update(values map[string]interface{}, escape bool) {
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

func (db *mySQL) addTableJoin(joinType string, tableName string, primaryKey string, foreignKey string) {
	q := Query{}
	q.On(db.checkReserved(primaryKey), "=", db.checkReserved(foreignKey), false)
	db.joins = append(db.joins, join{
		Type:  joinType,
		Table: db.checkReserved(tableName),
		Query: q})
}

func (db *mySQL) JoinTable(tableName string, primaryKey string, foreignKey string) {
	db.addTableJoin("JOIN", tableName, primaryKey, foreignKey)
}

func (db *mySQL) LeftJoinTable(tableName string, primaryKey string, foreignKey string) {
	db.addTableJoin("LEFT JOIN", tableName, primaryKey, foreignKey)
}

func (db *mySQL) addSubJoin(joinType string, subSql DB, alias string, primaryKey string, foreignKey string) {
	q := Query{}
	q.On(db.checkReserved(primaryKey), "=", db.checkReserved(foreignKey), false)
	tableName := fmt.Sprintf("(%s) %s", subSql.GenerateSelect(), db.checkReserved(alias))
	params := subSql.getParams()
	db.joins = append(db.joins, join{
		Type:   joinType,
		Table:  tableName,
		Query:  q,
		Params: params})
}

func (db *mySQL) JoinSub(subSql DB, alias string, primaryKey string, foreignKey string) {
	db.addSubJoin("JOIN", subSql, alias, primaryKey, foreignKey)
}

func (db *mySQL) LeftJoinSub(subSql DB, alias string, primaryKey string, foreignKey string) {
	db.addSubJoin("LEFT JOIN", subSql, alias, primaryKey, foreignKey)
}

func (db *mySQL) addQueryTableJoin(joinType string, tableName string, queryFunc queryFunc) {
	q := Query{}
	queryFunc(&q)
	db.joins = append(db.joins, join{
		Type:  joinType,
		Table: db.checkReserved(tableName),
		Query: q})
}

func (db *mySQL) JoinTableQuery(tableName string, queryFunc queryFunc) {
	db.addQueryTableJoin("JOIN", tableName, queryFunc)
}

func (db *mySQL) LeftJoinTableQuery(tableName string, queryFunc queryFunc) {
	db.addQueryTableJoin("LEFT JOIN", tableName, queryFunc)
}

func (db *mySQL) addQuerySubJoin(joinType string, subSql DB, alias string, queryFunc queryFunc) {
	q := Query{}
	queryFunc(&q)
	tableName := fmt.Sprintf("(%s) %s", subSql.GenerateSelect(), db.checkReserved(alias))
	params := subSql.getParams()
	db.joins = append(db.joins, join{
		Type:   joinType,
		Table:  tableName,
		Query:  q,
		Params: params})
}

func (db *mySQL) JoinSubQuery(subSql DB, alias string, queryFunc queryFunc) {
	db.addQuerySubJoin("JOIN", subSql, alias, queryFunc)
}

func (db *mySQL) LeftJoinSubQuery(subSql DB, alias string, queryFunc queryFunc) {
	db.addQuerySubJoin("LEFT JOIN", subSql, alias, queryFunc)
}

func (db *mySQL) Where(field string, comparator string, value interface{}, escape bool) {
	db.query.Where(db.checkReserved(field), comparator, value, escape)
}

func (db *mySQL) WhereNull(field string) {
	db.query.WhereNull(db.checkReserved(field))
}

func (db *mySQL) WhereNotNull(field string) {
	db.query.WhereNotNull(db.checkReserved(field))
}

func (db *mySQL) addWhereInList(inType string, field string, values []interface{}, escape bool) {
	if inType == "in" {
		db.query.WhereInList(db.checkReserved(field), values, escape)
	} else {
		db.query.WhereNotInList(db.checkReserved(field), values, escape)
	}
}

func (db *mySQL) WhereInList(field string, values []interface{}, escape bool) {
	db.addWhereInList("in", db.checkReserved(field), values, escape)
}

func (db *mySQL) WhereNotInList(field string, values []interface{}, escape bool) {
	db.addWhereInList("not in", db.checkReserved(field), values, escape)
}

func (db *mySQL) WhereInSub(field string, subSql DB) {
	db.query.WhereInSub(db.checkReserved(field), subSql)
}

func (db *mySQL) WhereNotInSub(field string, subSql DB) {
	db.query.WhereNotInSub(db.checkReserved(field), subSql)
}

func (db *mySQL) Or() {
	db.query.Or()
}
func (db *mySQL) And() {
	db.query.And()
}

func (db *mySQL) OpenBracket() {
	db.query.OpenBracket()
}

func (db *mySQL) CloseBracket() {
	db.query.CloseBracket()
}

func (db *mySQL) LimitBy(number int) {
	db.limitBy = number
}

func (db *mySQL) OffsetBy(number int) {
	db.offsetBy = number
}

func (db *mySQL) OrderBy(field string, direction string) {
	direction = strings.ToUpper(direction)
	if direction == "ASC" || direction == "DESC" {
		db.ordering = append(db.ordering, orderBy{
			Field:     db.checkReserved(field),
			Direction: direction,
		})
	}

}

func (db *mySQL) GroupBy(field ...string) {
	for _, f := range field {
		db.groupColumns = append(db.groupColumns, db.checkReserved(f))
	}

}

func (db *mySQL) GenerateSelect() string {
	var params []interface{}
	query := "SELECT "
	query += strings.Join(db.cols, ",")
	query += " FROM "
	query += fmt.Sprintf(" %s ", db.table)

	for _, j := range db.joins {
		params = append(params, j.Params...)
		query += fmt.Sprintf(" %s %s ON ", j.Type, j.Table)
		whereString, jParams, _ := j.Query.ApplyWheres()
		query += fmt.Sprintf(" %s ", whereString)
		params = append(params, jParams...)
	}

	if len(db.query.wheres) > 0 {
		whereString, newParams, _ := db.query.ApplyWheres()
		params = append(params, newParams...)
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
	db.params = params
	return query
}
func (db *mySQL) GenerateInsert() string {
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
func (db *mySQL) GenerateUpdate() string {
	query := fmt.Sprintf("UPDATE %s SET ", db.table)
	query += strings.Join(db.updateValues, ",")
	if len(db.query.wheres) > 0 {
		whereStr, newParams, _ := db.query.ApplyWheres()
		query += fmt.Sprintf(" WHERE %s ", whereStr)
		db.params = append(db.params, newParams...)
	}
	return query
}

func (db *mySQL) GenerateDelete() string {
	query := fmt.Sprintf("DELETE FROM %s ", db.table)
	if len(db.query.wheres) > 0 {
		whereStr, newParams, _ := db.query.ApplyWheres()
		query += fmt.Sprintf(" WHERE %s ", whereStr)
		db.params = append(db.params, newParams...)
	}
	return query
}

func (db *mySQL) Save() (sql.Result, error) {
	var query string
	if len(db.insertValues) > 0 || len(db.multiInsertValues) > 0 {
		query = db.GenerateInsert()
	} else if len(db.updateValues) > 0 {
		query = db.GenerateUpdate()
	}
	return db.executeNonQuery(query)
}
func (db *mySQL) Fetch() (*sql.Rows, context.CancelFunc, error) {

	return db.executeQuery(db.GenerateSelect())
}

func (db *mySQL) FetchConcurrent() (successChannel chan bool, startRowsChannel chan bool, rowChannel chan *sql.Rows, nextChannel chan bool, completeChannel chan bool, cancelChannel chan bool, errorChannel chan error) {
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

func (db *mySQL) Delete() (sql.Result, error) {
	return db.executeNonQuery(db.GenerateDelete())
}

func (db *mySQL) concExecuteQuery(query string, successChannel chan bool, startRowsChannel chan bool, rowChan chan *sql.Rows, nextChan chan bool, completeChan chan bool, cancelChan chan bool, errorChan chan error) {

	con := openConnections[db.databaseName]

	if db.parallel {
		newCon, err := db.openConnection()
		if err != nil {
			fmt.Println(err)
			errorChan <- err
			return
		}
		con = newCon
		defer con.Close()
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelFunc()
	results, err := con.QueryContext(ctx, query, db.params...)

	if err != nil {
		fmt.Println(err)
		errorChan <- err
		return
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

func (db *mySQL) executeQuery(query string) (*sql.Rows, context.CancelFunc, error) {

	con := openConnections[db.databaseName]

	if db.parallel {
		newCon, err := db.openConnection()
		if err != nil {
			fmt.Println(err)
			return nil, nil, err
		}
		con = newCon
		defer con.Close()
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 60*time.Second)
	results, err := con.QueryContext(ctx, query, db.params...)

	if err != nil {
		fmt.Println(err)
		defer cancelFunc()
		return nil, nil, err
	}
	return results, cancelFunc, nil
}

func (db *mySQL) executeNonQuery(query string) (sql.Result, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelFunc()
	con := openConnections[db.databaseName]

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
