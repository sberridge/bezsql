package bezsql

import (
	"errors"
	"fmt"
)

var connectionConfigs map[string]Config = make(map[string]Config)

func SetConnections(configs map[string]Config) {
	connectionConfigs = configs
}

func Open(database string) (DB, error) {

	if _, exists := connectionConfigs[database]; !exists {
		return nil, errors.New("database not found")
	}

	var db DB
	dbConfig := connectionConfigs[database]
	dbType := dbConfig.Type
	switch dbType {
	case "MySQL":
		db = &mySQL{}
	case "SQLServer":
		db = &sQLServer{}
		db.SetParamPrefix("param")
	}

	_, err := db.connect(database, dbConfig)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	return db, nil
}
