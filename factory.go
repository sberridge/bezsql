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
		db = &MySQL{}
	case "SQLServer":
		db = &SQLServer{}
		db.SetParamPrefix("param")
	}

	_, err := db.Connect(database, dbConfig)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	return db, nil
}
