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
	if !db.DoesTableExist("users") {
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
		_, closeCreateTable := db.RawNonQuery(createTableQuery, params)
		closeCreateTable()
	} else {
		_, closeTruncate := db.RawNonQuery("TRUNCATE TABLE users;", params)
		closeTruncate()
	}
	if !db.DoesTableExist("titles") {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `titles` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`title` VARCHAR(10) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, closeCreateTable := db.RawNonQuery(createTableQuery, params)
		closeCreateTable()
	} else {
		_, closeTruncate := db.RawNonQuery("TRUNCATE TABLE titles;", params)
		closeTruncate()
	}
	if !db.DoesTableExist("genders") {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `genders` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`gender` VARCHAR(10) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, closeCreateTable := db.RawNonQuery(createTableQuery, params)
		closeCreateTable()
	} else {
		_, closeTruncate := db.RawNonQuery("TRUNCATE TABLE genders;", params)
		closeTruncate()
	}
	if !db.DoesTableExist("countries") {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `countries` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`country` VARCHAR(50) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, closeCreateTable := db.RawNonQuery(createTableQuery, params)
		closeCreateTable()
	} else {
		_, closeTruncate := db.RawNonQuery("TRUNCATE TABLE countries;", params)
		closeTruncate()
	}
	if !db.DoesTableExist("cities") {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `cities` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`city` VARCHAR(50) NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, closeCreateTable := db.RawNonQuery(createTableQuery, params)
		closeCreateTable()
	} else {
		_, closeTruncate := db.RawNonQuery("TRUNCATE TABLE cities;", params)
		closeTruncate()
	}
	if !db.DoesTableExist("user_settings") {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `user_settings` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`user_id` INT NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, closeCreateTable := db.RawNonQuery(createTableQuery, params)
		closeCreateTable()
	} else {
		_, closeTruncate := db.RawNonQuery("TRUNCATE TABLE user_settings;", params)
		closeTruncate()
	}
	if !db.DoesTableExist("parties") {
		createTableQuery := strings.Join([]string{
			"CREATE TABLE `parties` (",
			"`id` INT NOT NULL AUTO_INCREMENT,",
			"`date` DATETIME NOT NULL,",
			"`city_id` INT NOT NULL,",
			"PRIMARY KEY (`id`)",
			")",
			"COLLATE='utf8mb4_general_ci';",
		}, "")
		_, closeCreateTable := db.RawNonQuery(createTableQuery, params)
		closeCreateTable()
	} else {
		_, closeTruncate := db.RawNonQuery("TRUNCATE TABLE parties;", params)
		closeTruncate()
	}
	if !db.DoesTableExist("party_guests") {
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
		_, closeCreateTable := db.RawNonQuery(createTableQuery, params)
		defer closeCreateTable()
	} else {
		_, closeTruncate := db.RawNonQuery("TRUNCATE TABLE party_guests;", params)
		closeTruncate()
	}

}

func TestSql(t *testing.T) {

}
