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
			"`country_id` int NOT NULL,",
			"`postcode` VARCHAR(100) NOT NULL,",
			"`street_address` VARCHAR(100) NOT NULL,",
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
		"city_id":        1,
		"country_id":     1,
		"postcode":       "DE76 YAS",
		"street_address": "123 Fake Street",
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
		"phone_number":   "07434534534534",
		"city_id":        1,
		"country_id":     1,
		"postcode":       "DE76 YAS",
		"street_address": "123 Fake Street",
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
	if rowNum != 2 {
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
	if rowNum != 1 {
		t.Fatalf("Failed fetching rows, expected 1 got %d", rowNum)
	}
}
