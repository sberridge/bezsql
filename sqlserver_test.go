package bezsql

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

func init() {
	SetConnections(map[string]Config{
		"sqlserver_test": {
			Type:     "SQLServer",
			Host:     "localhost",
			Port:     1433,
			Username: "sa",
			Password: "SuperSecurePassword!",
			Database: "test",
		},
		"mysql_test": {
			Type:     "MySQL",
			Host:     "localhost",
			Port:     3306,
			Username: "root",
			Password: "",
			Database: "test",
		},
	})
	db, err := Open("sqlserver_test")
	if err != nil {
		panic("no test database found")
	}
	var params []interface{}
	if t, err := db.DoesTableExist("users"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE [dbo].[users](",
			"[id] [int] IDENTITY(1,1) NOT NULL,",
			"[title_id] [int] NOT NULL,",
			"[first_name] [varchar](50) NOT NULL,",
			"[surname] [varchar](50) NOT NULL,",
			"[email] [varchar](200) NULL,",
			"[gender_id] [int] NOT NULL,",
			"[date_of_birth] [datetime] NOT NULL,",
			"[phone_number] [varchar](50) NOT NULL,",
			"[city_id] [int] NOT NULL,",
			"[country_id] [int] NULL,",
			"[postcode] [varchar](100) NOT NULL,",
			"[street_address] [varchar](100) NOT NULL,",
			"[active] [tinyint] DEFAULT 0,",
			"CONSTRAINT [PK_users] PRIMARY KEY CLUSTERED",
			"(",
			"[id] ASC",
			")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]",
			") ON [PRIMARY]",
		}, "")
		_, er := db.RawNonQuery(createTableQuery, params)
		if er != nil {
			fmt.Println(er)
		}
	} else {
		db.RawNonQuery("TRUNCATE TABLE users;", params)
	}

	users := [][]interface{}{
		{1, "Steve", "Berridge", "ste@ber.com", 1, "1993-07-12 00:00:00", "07434534534534", 3, 1, "DE76 YAS", "123 Fake Street", 1},
		{1, "Bob", "Briar", nil, 1, "1999-08-27 00:00:00", "07123564334555", 4, nil, "DE71 AXC", "14 Boller Road", 0},
		{1, "Sharon", "Pollard", "shar@pol.com", 2, "1967-03-12 00:00:00", "076453434553345", 2, 1, "DE71 AXC", "14 Boller Road", 0},
		{1, "Juliet", "Jones", "jules@jones.com", 2, "1985-06-01 00:00:00", "079874636334544", 1, 1, "ST54 POC", "1 Everet Avenue", 1},
	}

	insertUserDb, _ := db.NewQuery()
	insertUserDb.Table("users")
	insertUserDb.InsertMulti([]string{
		"title_id",
		"first_name",
		"surname",
		"email",
		"gender_id",
		"date_of_birth",
		"phone_number",
		"city_id",
		"country_id",
		"postcode",
		"street_address",
		"active",
	}, users, true)
	insertUserDb.Save()

	if t, err := db.DoesTableExist("titles"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE [dbo].[titles](",
			"[id] [int] IDENTITY(1,1) NOT NULL,",
			"[title] [varchar](10) NOT NULL,",
			"CONSTRAINT [PK_titles] PRIMARY KEY CLUSTERED",
			"(",
			"[id] ASC",
			")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]",
			") ON [PRIMARY]",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE titles;", params)
	}

	insertTitleDb, _ := db.NewQuery()
	insertTitleDb.Table("titles")
	insertTitleDb.Insert(map[string]interface{}{
		"title": "Mr",
	}, true)
	insertTitleDb.Save()

	if t, err := db.DoesTableExist("genders"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE [dbo].[genders](",
			"[id] [int] IDENTITY(1,1) NOT NULL,",
			"[gender] [varchar](10) NOT NULL,",
			"CONSTRAINT [PK_genders] PRIMARY KEY CLUSTERED",
			"(",
			"[id] ASC",
			")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]",
			") ON [PRIMARY]",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE genders;", params)
	}

	genders := [][]interface{}{
		{"Male"},
		{"Female"},
	}

	insertGenderDb, _ := db.NewQuery()
	insertGenderDb.Table("genders")
	insertGenderDb.InsertMulti([]string{
		"gender",
	}, genders, true)
	insertGenderDb.Save()

	if t, err := db.DoesTableExist("countries"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE [dbo].[countries](",
			"[id] [int] IDENTITY(1,1) NOT NULL,",
			"[country] [varchar](50) NOT NULL,",
			"CONSTRAINT [PK_countries] PRIMARY KEY CLUSTERED",
			"(",
			"[id] ASC",
			")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]",
			") ON [PRIMARY]",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE countries;", params)
	}

	insertCountryDb, _ := db.NewQuery()
	insertCountryDb.Table("countries")
	insertCountryDb.Insert(map[string]interface{}{
		"country": "United Kingdom",
	}, true)
	insertCountryDb.Save()

	if t, err := db.DoesTableExist("cities"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE [dbo].[cities](",
			"[id] [int] IDENTITY(1,1) NOT NULL,",
			"[city] [varchar](50) NOT NULL,",
			"CONSTRAINT [PK_cities] PRIMARY KEY CLUSTERED",
			"(",
			"[id] ASC",
			")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]",
			") ON [PRIMARY]",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE cities;", params)
	}

	cities := [][]interface{}{
		{"Derby"},
		{"Birmingham"},
		{"Burton-on-Trent"},
		{"London"},
	}

	insertCityDb, _ := db.NewQuery()
	insertCityDb.Table("cities")
	insertCityDb.InsertMulti([]string{
		"city",
	}, cities, true)
	insertCityDb.Save()

	if t, err := db.DoesTableExist("user_settings"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE [dbo].[user_settings](",
			"[id] [int] IDENTITY(1,1) NOT NULL,",
			"[user_id] [int] NOT NULL,",
			"CONSTRAINT [PK_user_settings] PRIMARY KEY CLUSTERED",
			"(",
			"[id] ASC",
			")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]",
			") ON [PRIMARY]",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE user_settings;", params)
	}

	insertUserSettingsDb, _ := db.NewQuery()
	insertUserSettingsDb.Table("user_settings")
	insertUserSettingsDb.Insert(map[string]interface{}{
		"user_id": 1,
	}, true)
	insertUserSettingsDb.Save()

	insertUserSettingsDb, _ = db.NewQuery()
	insertUserSettingsDb.Table("user_settings")
	insertUserSettingsDb.Insert(map[string]interface{}{
		"user_id": 2,
	}, true)
	insertUserSettingsDb.Save()

	if t, err := db.DoesTableExist("parties"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE [dbo].[parties](",
			"[id] [int] IDENTITY(1,1) NOT NULL,",
			"[date] [datetime] NOT NULL,",
			"[city_id] [int] NOT NULL,",
			"CONSTRAINT [PK_parties] PRIMARY KEY CLUSTERED",
			"(",
			"[id] ASC",
			")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]",
			") ON [PRIMARY]",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE parties;", params)
	}

	insertPartyDb, _ := db.NewQuery()
	insertPartyDb.Table("parties")
	insertPartyDb.Insert(map[string]interface{}{
		"date":    "2021-10-01 18:00:00",
		"city_id": 1,
	}, true)
	insertPartyDb.Save()

	if t, err := db.DoesTableExist("party_guests"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE [dbo].[party_guests](",
			"[id] [int] IDENTITY(1,1) NOT NULL,",
			"[party_id] [int] NOT NULL,",
			"[user_id] [int] NOT NULL,",
			"[accepted] [tinyint] NOT NULL DEFAULT 0,",
			"CONSTRAINT [PK_party_guests] PRIMARY KEY CLUSTERED",
			"(",
			"[id] ASC",
			")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]",
			") ON [PRIMARY]",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE party_guests;", params)
	}

	guests := [][]interface{}{
		{1, 1, false},
		{2, 1, false},
		{3, 1, true},
	}

	insertGuestDb, _ := db.NewQuery()
	insertGuestDb.Table("party_guests")
	insertGuestDb.InsertMulti([]string{
		"user_id",
		"party_id",
		"accepted",
	}, guests, true)
	insertGuestDb.Save()

}

func TestSQLServerSelect(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"id",
		"surname",
	})
	res, close, err := db.Fetch()
	if err != nil {
		t.Fatalf("Failed running query, got %s", err.Error())
	}
	defer close()
	rowNum := 0
	for res.Next() {
		var (
			id      int64
			surname string
		)
		rowNum++
		res.Scan(&id, &surname)
	}
	if rowNum != 4 {
		t.Fatalf("Failed fetching rows, expected 2 got %d", rowNum)
	}
}

func TestSQLServerSelectBasicWhere(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"id",
		"surname",
	})
	db.Where("id", "=", 1, true)
	res, close, err := db.Fetch()
	if err != nil {
		t.Fatalf("Failed running query, got %s", err.Error())
	}
	defer close()
	rowNum := 0
	for res.Next() {
		var (
			id      int64
			surname string
		)
		rowNum++
		res.Scan(&id, &surname)
		if id != 1 {
			t.Fatalf("Failed fetching correct row, expected id 1, got %d", id)
		}
	}
	if rowNum != 1 {
		t.Fatalf("Failed fetching rows, expected 1 got %d", rowNum)
	}
}

func TestSQLServerSelectComplexWhere(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"id",
		"first_name",
		"active",
		"date_of_birth",
	})
	db.OpenBracket()
	db.Where("active", "=", 1, true)
	db.Where("date_of_birth", ">", "1980-01-01 00:00:00", true)
	db.CloseBracket()
	db.Or()
	db.OpenBracket()
	db.Where("first_name", "=", "Sharon", true)
	db.CloseBracket()
	res, close, err := db.Fetch()
	if err != nil {
		t.Fatalf("Failed running query, got %s", err.Error())
	}
	defer close()
	rowNum := 0
	for res.Next() {
		var (
			id            int64
			first_name    string
			active        bool
			date_of_birth string
		)
		rowNum++
		res.Scan(&id, &first_name, &active, &date_of_birth)
		if !((active && date_of_birth > "1980-01-01 00:00:00") || (first_name == "Sharon")) {
			t.Fatalf("Found invalid rows, should be (active AND date of birth > 1980-01-01) OR (first_name = 'Sharon'), got %v, %s, %s", active, date_of_birth, first_name)
		}
	}
	if rowNum != 3 {
		t.Fatalf("Failed fetching rows, expected 3 got %d", rowNum)
	}
}

func TestSQLServerSubQuery(t *testing.T) {
	usersdb, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	usersdb.Table("users")
	usersdb.Cols([]string{
		"id",
	})

	mainDb, _ := usersdb.NewQuery()
	mainDb.TableSub(usersdb, "my_users")
	mainDb.Cols([]string{
		"my_users.id",
	})
	mainDb.Where("my_users.id", "=", 1, true)
	res, close, err := mainDb.Fetch()
	if err != nil {
		t.Fatalf("Failed running sub query, got %s", err.Error())
	}
	defer close()
	totalRows := 0
	for res.Next() {
		totalRows++
		var id int
		res.Scan(&id)
		if id != 1 {
			t.Fatalf("Expected id 1, got %d", id)
		}
	}
	if totalRows == 0 {
		t.Fatalf("No results returned")
	}
}

func TestSQLServerAggregate(t *testing.T) {
	countdb, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	countdb.Table("users")
	countdb.Cols([]string{
		countdb.Count("id", "counted"),
	})

	sumdb, _ := countdb.NewQuery()
	sumdb.Table("cities")
	sumdb.Cols([]string{
		sumdb.Sum("id", "summed"),
	})

	avgdb, _ := countdb.NewQuery()
	avgdb.Table("users")
	avgdb.Cols([]string{
		avgdb.Avg("gender_id", "avged"),
	})

	mindb, _ := countdb.NewQuery()
	mindb.Table("users")
	mindb.Cols([]string{
		mindb.Min("date_of_birth", "youngest"),
	})

	maxdb, _ := countdb.NewQuery()
	maxdb.Table("users")
	maxdb.Cols([]string{
		maxdb.Max("date_of_birth", "oldest"),
	})

	results := ConcurrentFetch(countdb, sumdb, avgdb, mindb, maxdb)

	for i, res := range results {
		switch i {
		case 0:
			//count
			if len(res.Errors) > 0 {
				t.Fatalf("Fetching count failed with errors %s", joinErrors(res.Errors))
			} else {
				totalRows := 0
				res.StartRowsChannel <- true
				done := false
				for {
					select {
					case row := <-res.RowChannel:
						totalRows++
						var count int
						row.Scan(&count)
						if count == 0 {
							t.Fatal("Count returned 0")
						}
						res.NextChannel <- true

					case <-res.CompleteChannel:
						done = true
					}
					if done {
						break
					}
				}
			}
		case 1:
			//sum
			if len(res.Errors) > 0 {
				t.Fatalf("Fetching sum failed with errors %s", joinErrors(res.Errors))
			} else {
				totalRows := 0
				res.StartRowsChannel <- true
				done := false
				for {
					select {
					case row := <-res.RowChannel:
						totalRows++
						var sum int
						row.Scan(&sum)
						if sum == 0 {
							t.Fatal("Sum returned 0")
						}
						res.NextChannel <- true

					case <-res.CompleteChannel:
						done = true
					}
					if done {
						break
					}
				}
			}
		case 2:
			//avg
			if len(res.Errors) > 0 {
				t.Fatalf("Fetching avg failed with errors %s", joinErrors(res.Errors))
			} else {
				totalRows := 0
				res.StartRowsChannel <- true
				done := false
				for {
					select {
					case row := <-res.RowChannel:
						totalRows++
						var avg float32
						row.Scan(&avg)
						if avg == 0 {
							t.Fatal("Avg returned 0")
						}
						res.NextChannel <- true

					case <-res.CompleteChannel:
						done = true
					}
					if done {
						break
					}
				}
			}
		case 3:
			//min
			if len(res.Errors) > 0 {
				t.Fatalf("Fetching min failed with errors %s", joinErrors(res.Errors))
			} else {
				totalRows := 0
				res.StartRowsChannel <- true
				done := false
				for {
					select {
					case row := <-res.RowChannel:
						totalRows++
						var min string
						row.Scan(&min)
						if min != "1967-03-12T00:00:00Z" {
							t.Fatalf("Failed fetching min date of birth, got %s", min)
						}
						res.NextChannel <- true

					case <-res.CompleteChannel:
						done = true
					}
					if done {
						break
					}
				}
			}
		case 4:
			//max
			if len(res.Errors) > 0 {
				t.Fatalf("Fetching max failed with errors %s", joinErrors(res.Errors))
			} else {
				totalRows := 0
				res.StartRowsChannel <- true
				done := false
				for {
					select {
					case row := <-res.RowChannel:
						totalRows++
						var max string
						row.Scan(&max)
						if max != "1999-08-27T00:00:00Z" {
							t.Fatalf("Failed fetching max date of birth, got %s", max)
						}
						res.NextChannel <- true

					case <-res.CompleteChannel:
						done = true
					}
					if done {
						break
					}
				}
			}
		}
	}
}

func TestSQLServerSelectWhereNull(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"id",
		"surname",
		"email",
	})
	db.WhereNull("email")
	res, close, err := db.Fetch()
	if err != nil {
		t.Fatalf("Failed running query, got %s", err.Error())
	}
	defer close()
	rowNum := 0
	for res.Next() {
		var (
			id      int64
			surname string
			email   sql.NullString
		)
		rowNum++
		res.Scan(&id, &surname, &email)
		if email.Valid {
			t.Fatal("Failed fetching where null")
		}
	}
	if rowNum != 1 {
		t.Fatalf("Failed fetching rows, expected 1 got %d", rowNum)
	}
}

func TestSQLServerSelectWhereNotNull(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"id",
		"surname",
		"email",
	})
	db.WhereNotNull("email")
	res, close, err := db.Fetch()
	if err != nil {
		t.Fatalf("Failed running query, got %s", err.Error())
	}
	defer close()
	rowNum := 0
	for res.Next() {
		var (
			id      int64
			surname string
			email   sql.NullString
		)
		rowNum++
		res.Scan(&id, &surname, &email)
		if !email.Valid {
			t.Fatal("Failed fetching where not null")
		}
	}
	if rowNum != 3 {
		t.Fatalf("Failed fetching rows, expected 3 got %d", rowNum)
	}
}

func TestSQLServerSelectWhereInList(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("cities")
	db.Cols([]string{
		"id",
		"city",
	})
	db.WhereInList("city", []interface{}{
		"Derby",
		"Birmingham",
	}, true)
	res, close, err := db.Fetch()
	if err != nil {
		t.Fatalf("Failed running query, got %s", err.Error())
	}
	defer close()
	rowNum := 0
	for res.Next() {
		var (
			id   int64
			city string
		)
		rowNum++
		res.Scan(&id, &city)
		if city != "Derby" && city != "Birmingham" {
			t.Fatalf("Invalid result returned, got %s", city)
		}
	}
	if rowNum != 2 {
		t.Fatalf("Failed fetching rows, expected 2 got %d", rowNum)
	}
}

func TestSQLServerSelectWhereNotInList(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("cities")
	db.Cols([]string{
		"id",
		"city",
	})
	db.WhereNotInList("city", []interface{}{
		"Derby",
		"Birmingham",
	}, true)
	res, close, err := db.Fetch()
	if err != nil {
		t.Fatalf("Failed running query, got %s", err.Error())
	}
	defer close()
	rowNum := 0
	for res.Next() {
		var (
			id   int64
			city string
		)
		rowNum++
		res.Scan(&id, &city)
		if city != "Burton-on-Trent" && city != "London" {
			t.Fatalf("Invalid result returned, got %s", city)
		}
	}
	if rowNum != 2 {
		t.Fatalf("Failed fetching rows, expected 2 got %d", rowNum)
	}
}

func TestSQLServerSelectWhereInSub(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"gender_id",
	})

	subDb, _ := db.NewQuery()
	subDb.SetParamPrefix("gendersSub")
	subDb.Table("genders")
	subDb.Cols([]string{
		"id",
	})
	subDb.Where("gender", "=", "Female", true)

	db.WhereInSub("gender_id", subDb)

	res, closeFunc, _ := db.Fetch()
	defer closeFunc()
	rowNum := 0
	for res.Next() {
		rowNum++
		var (
			gender_id int32
		)
		res.Scan(&gender_id)
		if gender_id != 2 {
			t.Fatalf("Invalid result returned, expected 2 got %d", gender_id)
		}
	}
	if rowNum == 0 {
		t.Fatalf("No results found")
	}

}

func TestSQLServerSelectWhereNotInSub(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"gender_id",
	})

	subDb, _ := db.NewQuery()
	subDb.SetParamPrefix("gendersSub")
	subDb.Table("genders")
	subDb.Cols([]string{
		"id",
	})
	subDb.Where("gender", "=", "Female", true)

	db.WhereNotInSub("gender_id", subDb)

	res, closeFunc, _ := db.Fetch()
	defer closeFunc()
	rowNum := 0
	for res.Next() {
		rowNum++
		var (
			gender_id int32
		)
		res.Scan(&gender_id)
		if gender_id != 1 {
			t.Fatalf("Invalid result returned, expected 1 got %d", gender_id)
		}
	}
	if rowNum == 0 {
		t.Fatalf("No results found")
	}

}

func TestSQLServerInsertAndDelete(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("cities")
	db.Insert(map[string]interface{}{
		"city": "Belper",
	}, true)
	res, _ := db.Save()

	if r, _ := res.RowsAffected(); r == 0 {
		t.Fatalf("Record not inserted")
	} else {
		id, _ := res.LastInsertId()
		deleteDb, _ := db.NewQuery()
		deleteDb.Table("cities")
		deleteDb.Where("id", "=", id, true)
		res, _ := deleteDb.Delete()
		if r, _ := res.RowsAffected(); r != 1 {
			t.Fatalf("Should have deleted 1 record, actually deleted %d", r)
		}
	}
}

func TestSQLServerInsertMultiAndDelete(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("cities")
	db.InsertMulti([]string{
		"city",
	}, [][]interface{}{
		{"Hartlepool"},
		{"Liverpool"},
		{"Blackpool"},
	}, true)
	res, err := db.Save()
	if err != nil {
		t.Fatalf("Failed inserting multiple rows, got %s", err.Error())
	}
	rows, _ := res.RowsAffected()
	if rows != 3 {
		t.Fatalf("Expected to insert 3 rows, got %d", rows)
	}
	insertId, _ := res.LastInsertId()

	delDb, _ := db.NewQuery()
	delDb.Table("cities")
	delDb.Where("id", ">=", insertId, true)
	delDb.Where("id", "<", insertId+rows, true)
	delRes, err := delDb.Delete()
	if err != nil {
		t.Fatalf("Failed deleting multiple rows, got %s", err.Error())
	}
	rows, _ = delRes.RowsAffected()
	if rows != 3 {
		t.Fatalf("Expected to delete 3 rows, got %d", rows)
	}

}

func TestSQLServerUpdate(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("party_guests")
	db.Update(map[string]interface{}{
		"accepted": 1,
	}, true)
	db.Where("user_id", "=", 1, true)
	db.Where("party_id", "=", 1, true)
	res, _ := db.Save()
	if r, _ := res.RowsAffected(); r != 1 {
		t.Fatalf("Should have updated 1 record, actually updated %d", r)
	}
}

func TestSQLServerStandardJoin(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"users.city_id",
		"cities.id",
	})
	db.JoinTable("cities", "cities.id", "users.city_id")
	res, closeFunc, _ := db.Fetch()
	defer closeFunc()
	rowNum := 0
	for res.Next() {
		rowNum++
		var (
			city_id int32
			id      int32
		)
		res.Scan(&city_id, &id)
		if city_id != id {
			t.Fatalf("Joined table IDs do not match, got %d and %d", city_id, id)
		}
	}
	if rowNum == 0 {
		t.Fatalf("No rows found")
	}
}

func TestSQLServerStandardLeftJoin(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"users.country_id",
		"countries.id",
	})
	db.LeftJoinTable("countries", "countries.id", "users.country_id")
	res, closeFunc, _ := db.Fetch()
	defer closeFunc()
	rowNum := 0
	foundNull := false
	for res.Next() {
		rowNum++
		var (
			country_id sql.NullInt32
			id         sql.NullInt32
		)
		res.Scan(&country_id, &id)
		if country_id.Valid && !id.Valid {
			t.Fatalf("Found records where id's don't match, found %d and %d", country_id.Int32, id.Int32)
		}
		if !id.Valid {
			foundNull = true
		}
	}
	if !foundNull {
		t.Fatalf("Did not find record with a null country_id")
	}
	if rowNum == 0 {
		t.Fatalf("No rows found")
	}
}

func TestSQLServerQueryJoin(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"users.id",
		"users.first_name",
		"party_guests.accepted",
		"party_guests.party_id",
	})
	db.JoinTableQuery("party_guests", func(q *Query) {
		q.On("users.id", "=", "party_guests.user_id", false)
		q.On("party_guests.accepted", "=", 1, true)
	})
	res, closeFunc, _ := db.Fetch()
	defer closeFunc()
	rowNum := 0

	for res.Next() {
		rowNum++
		var (
			id         int32
			first_name string
			accepted   bool
			party_id   int32
		)
		res.Scan(&id, &first_name, &accepted, &party_id)
		if !accepted {
			t.Fatalf("Found unaccepted party guest, user %d, party %d", id, party_id)
		}
	}
	if rowNum == 0 {
		t.Fatalf("No rows found")
	}
}

func TestSQLServerSubJoin(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"users.id",
		"users.first_name",
		"g.accepted",
	})
	subDb, _ := db.NewQuery()
	subDb.SetParamPrefix("partyGuestsSub")
	subDb.Table("party_guests")
	subDb.Cols([]string{
		"user_id",
		"accepted",
	})

	db.JoinSub(subDb, "g", "users.id", "g.user_id")
	res, closeFunc, _ := db.Fetch()
	defer closeFunc()
	rowNum := 0

	for res.Next() {
		rowNum++
		var (
			id         int32
			first_name string
			accepted   bool
		)
		res.Scan(&id, &first_name, &accepted)
		if id == 0 {
			t.Fatalf("Expected non-zero id, got %d", id)
		}
	}
	if rowNum == 0 {
		t.Fatalf("No rows found")
	}
}

func TestSQLServerOrdering(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"first_name",
	})
	db.OrderBy("first_name", "ASC")
	res, closeFunc, _ := db.Fetch()
	defer closeFunc()
	prevStr := ""
	for res.Next() {
		var first_name string
		res.Scan(&first_name)
		if first_name < prevStr {
			t.Fatalf("String should be lower than previous, got %s and %s", first_name, prevStr)
		}
		prevStr = first_name
	}
}

func TestSQLServerGrouping(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("party_guests")
	db.Cols([]string{
		db.Count("id", "number"),
		"user_id",
	})
	db.GroupBy("user_id")
	res, closeFunc, _ := db.Fetch()
	defer closeFunc()
	for res.Next() {
		var (
			number  int32
			user_id int32
		)
		res.Scan(&number, &user_id)
	}
}

func TestSQLServerLimitOffset(t *testing.T) {
	db, err := Open("sqlserver_test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"id",
	})
	db.OrderBy("id", "ASC")
	db.LimitBy(1)
	res, closeFunc, _ := db.Fetch()
	defer closeFunc()
	numRows := 0
	for res.Next() {
		numRows++
		var (
			id int
		)
		res.Scan(&id)
		if id != 1 {
			t.Fatalf("Expected ID 1, got %d", id)
		}
	}
	if numRows != 1 {
		t.Fatalf("Expected 1 row, got %d", numRows)
	}

	db.LimitBy(2)
	db.OffsetBy(1)

	res, closeFunc, _ = db.Fetch()
	defer closeFunc()
	numRows = 0
	ids := []string{}
	for res.Next() {
		numRows++
		var (
			id int
		)
		res.Scan(&id)
		ids = append(ids, fmt.Sprintf("%d", id))
	}
	if numRows != 2 {
		t.Fatalf("Expected 2 rows, got %d", numRows)
	}
	idStr := strings.Join(ids, ",")
	if idStr != "2,3" {
		t.Fatalf("Expected IDs 2 and 3, got %s", idStr)
	}

}

func TestSQLServerConcurrentFetch(t *testing.T) {
	db, _ := Open("sqlserver_test")
	db.Table("users")
	db.Cols([]string{
		"id",
		"first_name",
	})
	successChannel, startRowsChannel, rowChannel, nextChannel, completeChannel, _, errorChannel := db.FetchConcurrent()
	select {
	case err := <-errorChannel:
		t.Fatalf("Concurrent fetch errored with %s", err.Error())
	case <-successChannel:
		startRowsChannel <- true
		numRows := 0
		complete := false
		for {
			select {
			case row := <-rowChannel:
				numRows++
				var (
					id         int
					first_name string
				)
				row.Scan(&id, &first_name)
				nextChannel <- true
			case <-completeChannel:
				complete = true
			}
			if complete {
				break
			}
		}
		if numRows == 0 {
			t.Fatalf("No rows returned")
		}
	}
}

func TestSQLServerConcurrentMultiFetch(t *testing.T) {
	db1, _ := Open("sqlserver_test")
	db1.Table("users")
	db1.Cols([]string{
		"id",
		"first_name",
		"surname",
	})

	db2, _ := db1.NewQuery()
	db2.Table("cities")
	db2.Cols([]string{
		"id",
		"city",
	})

	db3, _ := db1.NewQuery()
	db3.Table("genders")
	db3.Cols([]string{
		"id",
		"gender",
	})

	db4, _ := db1.NewQuery()
	db4.Table("countries")
	db4.Cols([]string{
		"id",
		"country",
	})

	results := ConcurrentFetch(db1, db2, db3, db4)
	if len(results) != 4 {
		t.Fatalf("Expected 4 sets of results, got %d", len(results))
	}
	for i, res := range results {
		if i == 0 {
			numRows := 0
			if len(res.Errors) > 0 {
				t.Fatalf("First query errored %s", joinErrors(res.Errors))
			}
			complete := false
			res.StartRowsChannel <- true
			for {
				select {
				case row := <-res.RowChannel:
					numRows++
					var (
						id         int32
						first_name string
						surname    string
					)
					row.Scan(&id, &first_name, &surname)
					res.NextChannel <- true
				case <-res.CompleteChannel:
					complete = true
				}
				if complete {
					break
				}
			}
			if numRows == 0 {
				t.Fatal("No results returned in first concurrent query")
			}
		} else if i == 1 {
			numRows := 0
			if len(res.Errors) > 0 {
				t.Fatalf("Second query errored %s", joinErrors(res.Errors))
			}
			complete := false
			res.StartRowsChannel <- true
			for {
				select {
				case row := <-res.RowChannel:
					numRows++
					var (
						id   int32
						city string
					)
					row.Scan(&id, &city)
					res.NextChannel <- true
				case <-res.CompleteChannel:
					complete = true
				}
				if complete {
					break
				}
			}
			if numRows == 0 {
				t.Fatal("No results returned in second concurrent query")
			}
		} else if i == 2 {
			numRows := 0
			if len(res.Errors) > 0 {
				t.Fatalf("Third query errored %s", joinErrors(res.Errors))
			}
			complete := false
			res.StartRowsChannel <- true
			for {
				select {
				case row := <-res.RowChannel:
					numRows++
					var (
						id     int32
						gender string
					)
					row.Scan(&id, &gender)
					res.NextChannel <- true
				case <-res.CompleteChannel:
					complete = true
				}
				if complete {
					break
				}
			}
			if numRows == 0 {
				t.Fatal("No results returned in third concurrent query")
			}
		} else if i == 3 {
			if len(res.Errors) > 0 {
				t.Fatalf("Third query errored %s", joinErrors(res.Errors))
			}
			res.CancelChannel <- true
		}
	}
}

func runSQLServerConcurrent() {

	queries := []DB{}
	for i := 0; i < 20; i++ {
		db, _ := Open("sqlserver_test")
		db.Table("users")
		db.Cols([]string{
			"id",
			"first_name",
			"surname",
		})
		queries = append(queries, db)
	}

	res := ConcurrentFetch(queries...)
	for _, r := range res {
		if len(r.Errors) == 0 {
			//fmt.Println("test!!")
			r.StartRowsChannel <- true
			done := false
			for {
				select {
				case row := <-r.RowChannel:
					var (
						id         int
						first_name string
						surname    string
					)
					row.Scan(&id, &first_name, &surname)
					r.NextChannel <- true
				case <-r.CompleteChannel:
					done = true
				}
				if done {
					break
				}
			}
			//r.CancelChannel <- true
		} else {
			fmt.Println(r.Errors)
			fmt.Println("oh no")
		}
	}
}

func runSQLServerNonConcurrent() {
	for i := 0; i < 20; i++ {
		db, _ := Open("sqlserver_test")
		db.Table("users")
		db.Cols([]string{
			"id",
			"first_name",
			"surname",
		})
		_, c, _ := db.Fetch()
		defer c()
	}
}

func BenchmarkSQLServerConc(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runSQLServerConcurrent()
	}
}

func BenchmarkSQLServerNonConc(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runSQLServerNonConcurrent()
	}
}
