package bezsql

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

func init() {
	SetConnections(map[string]Config{
		"test": {
			Type:     "MySQL",
			Host:     "localhost",
			Port:     3306,
			Username: "root",
			Password: "",
			Database: "test",
		},
	})
	db, err := Open("test")
	if err != nil {
		panic("no test database found")
	}

	var params []interface{}
	if t, err := db.DoesTableExist("users"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE IF NOT EXISTS `users` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`title_id` INT NOT NULL,",
			"`first_name` VARCHAR(50) NOT NULL,",
			"`surname` VARCHAR(50) NOT NULL,",
			"`email` VARCHAR(200) DEFAULT NULL,",
			"`gender_id` int NOT NULL,",
			"`date_of_birth` DATETIME NOT NULL,",
			"`phone_number` VARCHAR(50) NOT NULL,",
			"`city_id` int NOT NULL,",
			"`country_id` int DEFAULT NULL,",
			"`postcode` VARCHAR(100) NOT NULL,",
			"`street_address` VARCHAR(100) NOT NULL,",
			"`active` TINYINT(1) DEFAULT 0,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, er := db.RawNonQuery(createTableQuery, params)
		if er != nil {
			fmt.Println(er)
		}
	} else {
		db.RawNonQuery("TRUNCATE TABLE users;", params)
	}

	insertUserDb, _ := db.Clone()
	insertUserDb.Table("users")
	insertUserDb.Insert(map[string]interface{}{
		"title_id":       1,
		"first_name":     "Steve",
		"surname":        "Berridge",
		"email":          "ste@ber.com",
		"gender_id":      1,
		"date_of_birth":  "1993-07-12 00:00:00",
		"phone_number":   "07434534534534",
		"city_id":        3,
		"country_id":     1,
		"postcode":       "DE76 YAS",
		"street_address": "123 Fake Street",
		"active":         1,
	}, true)
	insertUserDb.Save()

	insertUserDb, _ = db.Clone()
	insertUserDb.Table("users")
	insertUserDb.Insert(map[string]interface{}{
		"title_id":       1,
		"first_name":     "Bob",
		"surname":        "Briar",
		"email":          nil,
		"gender_id":      1,
		"date_of_birth":  "1999-08-27 00:00:00",
		"phone_number":   "07123564334555",
		"city_id":        4,
		"country_id":     nil,
		"postcode":       "DE71 AXC",
		"street_address": "14 Boller Road",
		"active":         0,
	}, true)
	insertUserDb.Save()

	insertUserDb, _ = db.Clone()
	insertUserDb.Table("users")
	insertUserDb.Insert(map[string]interface{}{
		"title_id":       1,
		"first_name":     "Sharon",
		"surname":        "Pollard",
		"email":          "shar@pol.com",
		"gender_id":      2,
		"date_of_birth":  "1967-03-12 00:00:00",
		"phone_number":   "076453434553345",
		"city_id":        2,
		"country_id":     1,
		"postcode":       "DE71 AXC",
		"street_address": "14 Boller Road",
		"active":         0,
	}, true)
	insertUserDb.Save()

	insertUserDb, _ = db.Clone()
	insertUserDb.Table("users")
	insertUserDb.Insert(map[string]interface{}{
		"title_id":       1,
		"first_name":     "Juliet",
		"surname":        "Jones",
		"email":          "jules@jones.com",
		"gender_id":      2,
		"date_of_birth":  "1985-06-01 00:00:00",
		"phone_number":   "079874636334544",
		"city_id":        1,
		"country_id":     1,
		"postcode":       "ST54 POC",
		"street_address": "1 Everet Avenue",
		"active":         1,
	}, true)
	insertUserDb.Save()

	if t, err := db.DoesTableExist("titles"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `titles` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`title` VARCHAR(10) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE titles;", params)
	}

	insertTitleDb, _ := db.Clone()
	insertTitleDb.Table("titles")
	insertTitleDb.Insert(map[string]interface{}{
		"title": "Mr",
	}, true)
	insertTitleDb.Save()

	if t, err := db.DoesTableExist("genders"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `genders` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`gender` VARCHAR(10) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE genders;", params)
	}

	insertGenderDb, _ := db.Clone()
	insertGenderDb.Table("genders")
	insertGenderDb.Insert(map[string]interface{}{
		"gender": "Male",
	}, true)
	insertGenderDb.Save()

	insertGenderDb, _ = db.Clone()
	insertGenderDb.Table("genders")
	insertGenderDb.Insert(map[string]interface{}{
		"gender": "Female",
	}, true)
	insertGenderDb.Save()

	if t, err := db.DoesTableExist("countries"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `countries` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`country` VARCHAR(50) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE countries;", params)
	}

	insertCountryDb, _ := db.Clone()
	insertCountryDb.Table("countries")
	insertCountryDb.Insert(map[string]interface{}{
		"country": "United Kingdom",
	}, true)
	insertCountryDb.Save()

	if t, err := db.DoesTableExist("cities"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `cities` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`city` VARCHAR(50) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE cities;", params)
	}

	insertCityDb, _ := db.Clone()
	insertCityDb.Table("cities")
	insertCityDb.Insert(map[string]interface{}{
		"city": "Derby",
	}, true)
	insertCityDb.Save()

	insertCityDb, _ = db.Clone()
	insertCityDb.Table("cities")
	insertCityDb.Insert(map[string]interface{}{
		"city": "Birmingham",
	}, true)
	insertCityDb.Save()

	insertCityDb, _ = db.Clone()
	insertCityDb.Table("cities")
	insertCityDb.Insert(map[string]interface{}{
		"city": "Burton-on-Trent",
	}, true)
	insertCityDb.Save()

	insertCityDb, _ = db.Clone()
	insertCityDb.Table("cities")
	insertCityDb.Insert(map[string]interface{}{
		"city": "London",
	}, true)
	insertCityDb.Save()

	if t, err := db.DoesTableExist("user_settings"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `user_settings` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`user_id` INT NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE user_settings;", params)
	}

	insertUserSettingsDb, _ := db.Clone()
	insertUserSettingsDb.Table("user_settings")
	insertUserSettingsDb.Insert(map[string]interface{}{
		"user_id": 1,
	}, true)
	insertUserSettingsDb.Save()

	insertUserSettingsDb, _ = db.Clone()
	insertUserSettingsDb.Table("user_settings")
	insertUserSettingsDb.Insert(map[string]interface{}{
		"user_id": 2,
	}, true)
	insertUserSettingsDb.Save()

	if t, err := db.DoesTableExist("parties"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `parties` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`date` DATETIME NOT NULL,",
			"`city_id` INT NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE parties;", params)
	}

	insertPartyDb, _ := db.Clone()
	insertPartyDb.Table("parties")
	insertPartyDb.Insert(map[string]interface{}{
		"date":    "2021-10-01 18:00:00",
		"city_id": 1,
	}, true)
	insertPartyDb.Save()

	if t, err := db.DoesTableExist("party_guests"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `party_guests` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`user_id` INT NOT NULL,",
			"`party_id` INT NOT NULL,",
			"`accepted` TINYINT(1) NOT NULL DEFAULT 0,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		db.RawNonQuery(createTableQuery, params)
	} else {
		db.RawNonQuery("TRUNCATE TABLE party_guests;", params)
	}

	insertGuestDb, _ := db.Clone()
	insertGuestDb.Table("party_guests")
	insertGuestDb.Insert(map[string]interface{}{
		"user_id":  1,
		"party_id": 1,
		"accepted": false,
	}, true)
	insertGuestDb.Save()

	insertGuestDb, _ = db.Clone()
	insertGuestDb.Table("party_guests")
	insertGuestDb.Insert(map[string]interface{}{
		"user_id":  2,
		"party_id": 1,
		"accepted": false,
	}, true)
	insertGuestDb.Save()

	insertGuestDb, _ = db.Clone()
	insertGuestDb.Table("party_guests")
	insertGuestDb.Insert(map[string]interface{}{
		"user_id":  3,
		"party_id": 1,
		"accepted": true,
	}, true)
	insertGuestDb.Save()

}

func TestSelect(t *testing.T) {
	db, err := Open("test")
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

func TestSelectBasicWhere(t *testing.T) {
	db, err := Open("test")
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

func TestSelectComplexWhere(t *testing.T) {
	db, err := Open("test")
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

func TestSelectWhereNull(t *testing.T) {
	db, err := Open("test")
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

func TestSelectWhereNotNull(t *testing.T) {
	db, err := Open("test")
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

func TestSelectWhereInList(t *testing.T) {
	db, err := Open("test")
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

func TestSelectWhereNotInList(t *testing.T) {
	db, err := Open("test")
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

func TestSelectWhereInSub(t *testing.T) {
	db, err := Open("test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"gender_id",
	})

	subDb, _ := db.Clone()
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

func TestSelectWhereNotInSub(t *testing.T) {
	db, err := Open("test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"gender_id",
	})

	subDb, _ := db.Clone()
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

func TestInsertAndDelete(t *testing.T) {
	db, err := Open("test")
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
		deleteDb, _ := db.Clone()
		deleteDb.Table("cities")
		deleteDb.Where("id", "=", id, true)
		res, _ := deleteDb.Delete()
		if r, _ := res.RowsAffected(); r != 1 {
			t.Fatalf("Should have deleted 1 record, actually deleted %d", r)
		}
	}
}

func TestUpdate(t *testing.T) {
	db, err := Open("test")
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

func TestStandardJoin(t *testing.T) {
	db, err := Open("test")
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

func TestStandardLeftJoin(t *testing.T) {
	db, err := Open("test")
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

func TestQueryJoin(t *testing.T) {
	db, err := Open("test")
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

func TestSubJoin(t *testing.T) {
	db, err := Open("test")
	if err != nil {
		t.Fatalf("Failed opening database, got %s", err.Error())
	}
	db.Table("users")
	db.Cols([]string{
		"users.id",
		"users.first_name",
		"g.accepted",
	})
	subDb, _ := db.Clone()
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

func TestOrdering(t *testing.T) {
	db, err := Open("test")
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
