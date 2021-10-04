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

To being a query you can use the Open function to return a Database struct which can be used to build and run a query.

```go
db, err := bezsql.Open("test")
```

The argument passed into this function determines which database is returned as specified by the connections that are set up.

### Run a Basic Query

```go
db.Table("users")
db.Cols([]string{
    "id",
    "username",
})
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
db.WhereInList("field3",[]interface{}{
    "val1",
    "val2",
}, true) // WHERE field3 IN (?,?)
```