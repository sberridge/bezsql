package bezsql

import (
	"errors"
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
	if dbType == "MySQL" {
		db = &MySQL{}
		db.Connect(database, dbConfig)
	}

	return db, nil
}
