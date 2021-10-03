package bezsql

import (
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
	if t, err := db.DoesTableExist("users"); err != nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE IF NOT EXISTS `users` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`title_id` INT NOT NULL,",
			"`first_name` VARCHAR(50) NOT NULL,",
			"`surname` VARCHAR(50) NOT NULL,",
			"`email` VARCHAR(200) NOT NULL,",
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
		_, closeCreateTable, _ := db.RawNonQuery(createTableQuery, params)
		defer closeCreateTable()
	} else {
		_, closeTruncate, _ := db.RawNonQuery("TRUNCATE TABLE users;", params)
		defer closeTruncate()
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
	_, closeFunc, _ := insertUserDb.Save()
	defer closeFunc()

	insertUserDb, _ = db.Clone()
	insertUserDb.Table("users")
	insertUserDb.Insert(map[string]interface{}{
		"title_id":       1,
		"first_name":     "Bob",
		"surname":        "Briar",
		"email":          "bob@brii.com",
		"gender_id":      1,
		"date_of_birth":  "1999-08-27 00:00:00",
		"phone_number":   "07434534534534",
		"city_id":        1,
		"country_id":     1,
		"postcode":       "DE76 YAS",
		"street_address": "123 Fake Street",
	}, true)
	_, closeFunc, _ = insertUserDb.Save()
	defer closeFunc()

	if t, err := db.DoesTableExist("titles"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `titles` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`title` VARCHAR(10) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, closeCreateTable, _ := db.RawNonQuery(createTableQuery, params)
		defer closeCreateTable()
	} else {
		_, closeTruncate, _ := db.RawNonQuery("TRUNCATE TABLE titles;", params)
		defer closeTruncate()
	}

	insertTitleDb, _ := db.Clone()
	insertTitleDb.Table("titles")
	insertTitleDb.Insert(map[string]interface{}{
		"title": "Mr",
	}, true)
	_, closeFunc, _ = insertTitleDb.Save()
	defer closeFunc()

	if t, err := db.DoesTableExist("genders"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `genders` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`gender` VARCHAR(10) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, closeCreateTable, _ := db.RawNonQuery(createTableQuery, params)
		defer closeCreateTable()
	} else {
		_, closeTruncate, _ := db.RawNonQuery("TRUNCATE TABLE genders;", params)
		defer closeTruncate()
	}

	insertGenderDb, _ := db.Clone()
	insertGenderDb.Table("genders")
	insertGenderDb.Insert(map[string]interface{}{
		"gender": "Male",
	}, true)
	_, closeFunc, _ = insertGenderDb.Save()
	defer closeFunc()

	if t, err := db.DoesTableExist("countries"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `countries` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`country` VARCHAR(50) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, closeCreateTable, _ := db.RawNonQuery(createTableQuery, params)
		defer closeCreateTable()
	} else {
		_, closeTruncate, _ := db.RawNonQuery("TRUNCATE TABLE countries;", params)
		defer closeTruncate()
	}

	insertCountryDb, _ := db.Clone()
	insertCountryDb.Table("countries")
	insertCountryDb.Insert(map[string]interface{}{
		"country": "United Kingdom",
	}, true)
	_, closeFunc, _ = insertCountryDb.Save()
	defer closeFunc()

	if t, err := db.DoesTableExist("cities"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `cities` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`city` VARCHAR(50) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, closeCreateTable, _ := db.RawNonQuery(createTableQuery, params)
		defer closeCreateTable()
	} else {
		_, closeTruncate, _ := db.RawNonQuery("TRUNCATE TABLE cities;", params)
		defer closeTruncate()
	}

	insertCityDb, _ := db.Clone()
	insertCityDb.Table("cities")
	insertCityDb.Insert(map[string]interface{}{
		"city": "Derby",
	}, true)
	_, closeFunc, _ = insertCityDb.Save()
	defer closeFunc()

	if t, err := db.DoesTableExist("user_settings"); err == nil && !t {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `user_settings` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`user_id` INT NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, closeCreateTable, _ := db.RawNonQuery(createTableQuery, params)
		defer closeCreateTable()
	} else {
		_, closeTruncate, _ := db.RawNonQuery("TRUNCATE TABLE user_settings;", params)
		defer closeTruncate()
	}

	insertUserSettingsDb, _ := db.Clone()
	insertUserSettingsDb.Table("user_settings")
	insertUserSettingsDb.Insert(map[string]interface{}{
		"user_id": 1,
	}, true)
	_, closeFunc, _ = insertUserSettingsDb.Save()
	defer closeFunc()

	insertUserSettingsDb, _ = db.Clone()
	insertUserSettingsDb.Table("user_settings")
	insertUserSettingsDb.Insert(map[string]interface{}{
		"user_id": 2,
	}, true)
	_, closeFunc, _ = insertUserSettingsDb.Save()
	defer closeFunc()

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
		_, closeCreateTable, _ := db.RawNonQuery(createTableQuery, params)
		defer closeCreateTable()
	} else {
		_, closeTruncate, _ := db.RawNonQuery("TRUNCATE TABLE parties;", params)
		defer closeTruncate()
	}

	insertPartyDb, _ := db.Clone()
	insertPartyDb.Table("parties")
	insertPartyDb.Insert(map[string]interface{}{
		"date":    "2021-10-01 18:00:00",
		"city_id": 1,
	}, true)
	_, closeFunc, _ = insertPartyDb.Save()
	defer closeFunc()

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
		_, closeCreateTable, _ := db.RawNonQuery(createTableQuery, params)
		defer closeCreateTable()
	} else {
		_, closeTruncate, _ := db.RawNonQuery("TRUNCATE TABLE party_guests;", params)
		defer closeTruncate()
	}

	insertGuestDb, _ := db.Clone()
	insertGuestDb.Table("party_guests")
	insertGuestDb.Insert(map[string]interface{}{
		"user_id":  1,
		"party_id": 1,
		"accepted": false,
	}, true)
	_, closeFunc, _ = insertGuestDb.Save()
	defer closeFunc()

	insertGuestDb, _ = db.Clone()
	insertGuestDb.Table("party_guests")
	insertGuestDb.Insert(map[string]interface{}{
		"user_id":  2,
		"party_id": 1,
		"accepted": false,
	}, true)
	_, closeFunc, _ = insertGuestDb.Save()
	defer closeFunc()

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
		t.Fatalf("Failed fetching rows, expected 2 got %d", rowNum)
	}
}
