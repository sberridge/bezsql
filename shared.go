package bezsql

import "strings"

type QueryFunc func(*Query)

type join struct {
	Type       string
	Table      string
	Query      Query
	Params     []interface{}
	ParamNames []string
}

type orderBy struct {
	Field     string
	Direction string
}

func joinErrors(errors []error) string {
	errorStrings := []string{}
	for _, err := range errors {
		errorStrings = append(errorStrings, err.Error())
	}
	return strings.Join(errorStrings, ", ")
}
