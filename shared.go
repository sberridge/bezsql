package bezsql

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
