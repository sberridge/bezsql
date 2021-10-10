package bezsql

import (
	"fmt"
	"strings"
)

func init() {
	SetConnections(map[string]Config{
		"mssql_test": {
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
	db, err := Open("mssql_test")
	if err != nil {
		panic("no test database found")
	}
	db.SetParamPrefix("param")
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
			"[active] [tinyint](1) DEFAULT 0,",
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

	/* if t, err := db.DoesTableExist("titles"); err == nil && !t {
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

	insertTitleDb, _ := db.NewQuery()
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

	insertCountryDb, _ := db.NewQuery()
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

	insertPartyDb, _ := db.NewQuery()
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
	insertGuestDb.Save() */

}
