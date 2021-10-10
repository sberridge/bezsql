# bezsql
Go SQL query package for handling connections &amp; building and running queries.

Built as learning project for Golang.

Currently supports:

* MySQL / MariaDB
* SQL Server

## Usage

### Set Database Connections

Setting the available database connections is done using the SetConnections function.

You can define multiple database connections here.

```go
bezsql.SetConnections(map[string]bezsql.Config{
    "mysql": {
        Type:     "MySQL",
        Host:     "localhost",
        Port:     3306,
        Username: "root",
        Password: "",
        Database: "test",
    },
    "sql_server": {
        Type:     "SQLServer",
        Host:     "localhost",
        Port:     1433,
        Username: "sa",
        Password: "sapassword",
        Database: "test",
    },
})
```

### Open Database Connection

To begin a query you can use the Open function to return a Database struct which can be used to build and run a query.

```go
db, err := bezsql.Open("test")
```

The argument passed into this function determines which database is returned as specified by the connections that are set up.

### Run a Basic Select Query

A basic select query involves choosing which table to select from and which fields to select.

The Fetch method is then used to execute the query and return the results.

```go
db.Table("users")
db.Cols([]string{
    "id",
    "username",
})

// res = *sql.rows
// closeFunc = context.CancelFunc
// err = error
res, closeFunc, err := db.Fetch()
defer closeFunc()

if err != nil {
    //error handling
}

for res.Next() {
    var (
        id int32
        username string
    )

    res.Scan(&id, &username)
    fmt.Println(id, username)
}
```

#### Fetch Concurrently

```go
db.Table("users")
db.Cols([]string{
    "id",
    "username",
})

successChannel, startRowsChannel, rowChannel, nextChannel, completeChannel, cancelChannel, errorChannel := db.FetchConcurrent()
select {
case err := <-errorChannel:
    //error handling
case <-successChannel:
    //query successful and can start recieving rows

    //start receiving
    startRowsChannel <- true

    //alternatively send on cancelChannel to cancel the query without looping through the results
    // cancelChannel <- true

    complete := false
    for {
        select {
        case row := <-rowChannel:
            //receives *sql.Rows on each iteration of Next

            var (
                id int
                username string
            )
            row.Scan(&id, &username)

            //request next row
            nextChannel <- true
        case <-completeChannel:
            //receives after all rows have returned
            //all rows received so exit loop
            complete = true
        }
        if complete {
            break
        }
    }
}
```

#### Fetch Multiple Queries Concurrently

A WIP function is available to allow multiple query results to be fetched concurrently.

```go
userDb, _ := bezsql.Open("test")
userDb.Table("users")
userDb.Cols([]string{
    "id",
    "username",
})
userDb.Where("date_created", ">", "2021-10-01", true)

//create new query on the same database
cityDb, _ := userDb.NewQuery()
cityDb.Table("cities")
cityDb.Cols([]string{
    "id",
    "city",
})

//returns map of int to result where int is the index of the original query
results := bezsql.ConcurrentFetch(userDb, cityDb)

userResults := results[0]
cityResults := results[1]


if len(userResults.Errors) > 0 {
    //error handling
}
completedQuery := false
//begin recieving rows
userResults.StartRowsChannel <- true
for {
    select {
    case row := <- userResults.RowChannel:
        //receives *sql.Rows on each iteration of Next
        var (
            id int
            username string
        )
        row.Scan(&id, &username)

        //request next row
        userResults.NextChannel <- true
    case <- userResults.CompleteChannel:
        //receives after final iteration of Next
        //all rows recieved so exit the loop
        completedQuery = true
    }

    if completedQuery {
        break
    }
}

if len(cityResults.Errors) > 0 {
    //error handling
}

//cancel the query
cityResults.CancelChannel <- true
```


### Adding Conditions

There are a few methods allowing the addition of conditions to a query.

```go
db.Table("users")
db.Cols([]string{
    "id",
    "username",
})

//basic comparison
// field, comparator, value, parameterize value
db.Where("field", "=", "value", true) // WHERE field = ?
db.Where("field2", ">", "1", false) // WHERE field > 1

//check null
db.WhereNull("nullable_field")
db.WhereNotNull("nullable_field2")

//where in
// field, value list, parametize values
db.WhereInList("field3", []interface{}{
    "val1",
    "val2",
}, true) // WHERE field3 IN (?,?)

// create new query on the same database
subWhereInDb := db.NewQuery()

//if using a database that requires named parameters then you can use the SetParamPrefix
//method to change the default prefix
subWhereInDb.SetParamPrefix("subParam")
// parameterize values will now be prefixed with "subParam", e.g. @subParam1 


subWhereInDb.Table("user_posts")
subWhereInDb.Cols([]string{
    subWhereInDb.Count("id", "number"),
    "user_id",
})
subWhereInDb.GroupBy("user_id")

// field, value list, parametize values
db.WhereInSub("field3", subWhereInDb) // WHERE field3 IN (SELECT ...)

//changing logic
db.Where("field", "=", 1, true)
db.Or()
db.Where("field2", "=", 2, true)
db.Where("field3", "=", 3, true)
db.And()
db.Where("field4", "=", 4, true)
//WHERE field = ? OR field2 = ? OR field3 = ? AND field4 = ?

//bracketting
db.OpenBracket()
db.Where("field", "=", 1, true)
db.Or()
db.Where("field2", "=", 2, true)
db.CloseBracket()
db.And()
db.OpenBracket()
db.Where("field3", "=", 3, true)
db.Or()
db.Where("field4", "=", 4, true)
db.CloseBracket()
//WHERE (field = ? OR field2 = ?) AND (field3 = ? OR field4 = ?)
```

### Grouping Results

Grouping is done with the GroupBy method.

```go
// variadic function accepting any number of group fields
db.GroupBy("field1", "field2")
//GROUP BY field1, field2
```

### Ordering Results

Ordering is done using the OrderBy method.

```go
db.OrderBy("date_created","ASC")
db.OrderBy("first_name", "ASC")
//ORDER BY date_created ASC, first_name ASC
```

### Limiting Results

Limiting is done with the LimitBy method.

```go
db.LimitBy(10)

// LIMIT 10
```

### Offsetting Results

Offsetting is done with the OffsetBy method.

```go
db.OffsetBy(1)

// OFFSET 1
```

### Table Joins

Joining tables is done using the following methods:

* JoinTable
* LeftJoinTable
* JoinTableQuery
* LeftJoinTableQuery
* JoinSub
* LeftJoinSub
* JoinSubQuery
* LeftJoinSubQuery

```go
//table, primary key, foreign key
db.JoinTable("user_settings", "user_settings.user_id", "users.id")

//table, primary key, foreign key
db.LeftJoinTable("posts", "users.id", "posts.user_id")


//table, query function
db.JoinTableQuery("posts", func(q *bezsql.Query) {
    //same syntax as a WHERE condition
    q.On("users.id", "=", "posts.user_id", false)
    q.On("posts.date", ">", "2021-10-06", true)
})

//table, query function
db.LeftJoinTableQuery("posts", func(q *bezsql.Query) {
    //same syntax as a WHERE condition
    q.On("users.id", "=", "posts.user_id", false)
    q.On("posts.date", ">", "2021-10-06", true)
})


subDb, _ := bezsql.Open("test")
subDb.Table("posts")
subDb.Cols([]string{
    subDb.Count("*", "number_of_posts"),
    "user_id"
})
subDb.GroupBy("user_id")

// DB struct, table alias, primary key, foreign key
db.JoinSub(subDb, "post_count", "users.id", "post_count.user_id")

// DB struct, table alias, primary key, foreign key
db.LeftJoinSub(subDb, "post_count", "users.id", "post_count.user_id")


//table, query function
db.JoinSubQuery(subDb, "post_count" func(q *bezsql.Query) {
    //same syntax as a WHERE condition
    q.On("users.id", "=", "post_count.user_id", false)
})

//table, query function
db.LeftJoinSubQuery(subDb, "post_count", func(q *bezsql.Query) {
    //same syntax as a WHERE condition
    q.On("users.id", "=", "post_count.user_id", false)
})
```



### Inserting Records

Inserting records is done using the following methods:

* Insert
* InsertMulti

The Save method is used to execute the query after setting the required values.

```go
userDb, _ := bezsql.Open("test")
userDb.Table("users")
//map of fields and values, whether or not to paramatise the values
userDb.Insert(map[string]interface{}{
    "username": "My User",
}, true)
res, err := userDb.Save()

if err != nil {
    //error handling
}

numOfRows, err := res.AffectedRows()

lastInsertId, err := res.LastInsertId()



//multi insert

multiInsertDb, err := userDb.NewQuery()

multiInsertDb.InsertMulti([]string{
    "username",
}, [][]interface{}{
    {"New User"},
    {"Second New User"},
})

res, err = multiInsertDb.Save()

numOfRows, err = res.AffectedRows()

firstInsertedId, err = res.LastInsertId()

//inserted ids = firstInsertedId -> (firstInsertedId + numOfRows) - 1

```


### Updating Records

Updates can be performed using the Update method combined with the various condition methods.

The Save method is used to execute the query.

```go
db.Update(map[string]interface{}{
    "name": "new name",
}, true)

db.Where("id", "=", 10, true)
result, err := db.Save()

if err != nil {
    //error handling
}

affectedRows := result.AffectedRows()
```

### Deleting Records

Deleting records can be done by initialising a query on a table, adding the required conditions, and then executing the Delete method.

```go
db.Table("users")
db.Where("id", "=", 10, true)
result, err := db.Delete()

if err != nil {
    //error handling
}

affectedRows := result.AffectedRows()
```