# bezsql
Go SQL query package for handling connections &amp; building and running queries.

Built as learning project for Golang.

## Usage

### Set Database Connections

Setting the available database connections is done using the SetConnections function.

You can define multiple database connections here.

```go
bezsql.SetConnections(map[string]bezsql.Config{
    "test": {
        Type:     "MySQL",
        Host:     "localhost",
        Port:     3306,
        Username: "root",
        Password: "",
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

### Run a Basic Query

A basic query involves choosing which table to select from and which fields to select.

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

### Ordering Results

Ordering is done using the OrderBy method.

```go
db.OrderBy("date_created","ASC")
db.OrderBy("first_name", "ASC")
//ORDER BY date_created ASC, first_name ASC
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

### Concurrent Fetching

A WIP function is available to allow query results to be fetched concurrently.

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

defer userResults.CloseFunc()
defer cityResults.CloseFunc()

for userResults.Results.Next() {
    var (
        id int32
        username string
    )
    userResults.Results.Scan(&id, &username)
}

for cityResults.Results.Next() {
    var (
        id int32
        city string
    )
    cityResults.Results.Scan(&id, &city)
}

```

### Inserting Records

Inserting records is done using the following methods:

* Insert
* InsertMulti

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


### Deleting Records