package bezsql

import (
	"fmt"
	"strings"
)

type where struct {
	Type       string
	Field      string
	Comparator string
	Value      interface{}
	Escape     bool
	Params     []interface{}
	NamedParam string
	ParamNames []string
}
type Query struct {
	useNamedParams bool
	paramPrefix    string
	paramNum       int
	wheres         []where
}

func (q *Query) SetParamPrefix(prefix string) {
	q.paramPrefix = prefix
	q.useNamedParams = true
}

func (q *Query) Where(field string, comparator string, value interface{}, escape bool) {
	w := where{
		Type:       "where",
		Field:      field,
		Comparator: comparator,
		Value:      value,
		Escape:     escape}
	if q.useNamedParams && escape {
		q.paramNum++
		w.NamedParam = fmt.Sprintf("%s%d", q.paramPrefix, q.paramNum)
	}
	q.wheres = append(q.wheres, w)
}

func (q *Query) On(field string, comparator string, value interface{}, escape bool) {
	q.Where(field, comparator, value, escape)
}

func (q *Query) WhereNull(field string) {
	w := where{
		Type:       "where",
		Field:      field,
		Comparator: "",
		Value:      "IS NULL",
		Escape:     false}
	q.wheres = append(q.wheres, w)
}

func (q *Query) WhereNotNull(field string) {
	w := where{
		Type:       "where",
		Field:      field,
		Comparator: "",
		Value:      "IS NOT NULL",
		Escape:     false}
	q.wheres = append(q.wheres, w)
}

func (q *Query) addWhereInList(inType string, field string, values []interface{}, escape bool) {
	valueString := ""
	var params []interface{}
	paramPrefixes := []string{}
	if !escape {
		stringValues := []string{}
		for _, value := range values {
			switch v := value.(type) {
			case string:
				stringValues = append(stringValues, v)
			}
		}

		valueString = fmt.Sprintf(" (%s) ", strings.Join(stringValues, ","))
	} else {
		var valueSlice []string
		for i := 0; i < len(values); i++ {
			if q.useNamedParams {
				q.paramNum++
				paramName := fmt.Sprintf("%s%d", q.paramPrefix, q.paramNum)
				paramPrefixes = append(paramPrefixes, paramName)
				valueSlice = append(valueSlice, fmt.Sprintf("@%s", paramName))
			} else {
				valueSlice = append(valueSlice, "?")
			}

		}
		valueString = fmt.Sprintf(" (%s) ", strings.Join(valueSlice, ","))
		params = values
	}
	q.wheres = append(q.wheres, where{
		Type:       "where",
		Field:      field,
		Comparator: inType,
		Value:      valueString,
		Escape:     false,
		Params:     params,
		ParamNames: paramPrefixes,
	})
}

func (q *Query) WhereInList(field string, values []interface{}, escape bool) {
	q.addWhereInList("IN", field, values, escape)
}

func (q *Query) WhereNotInList(field string, values []interface{}, escape bool) {
	q.addWhereInList("NOT IN", field, values, escape)
}

func (q *Query) addWhereInSub(inType string, field string, subQuery DB) {
	valueString := fmt.Sprintf(" (%s) ", subQuery.GenerateSelect())
	params := subQuery.GetParams()
	q.wheres = append(q.wheres, where{
		Type:       "where",
		Field:      field,
		Comparator: inType,
		Value:      valueString,
		Escape:     false,
		Params:     params,
	})
}

func (q *Query) WhereInSub(field string, subQuery DB) {
	q.addWhereInSub("IN", field, subQuery)
}

func (q *Query) WhereNotInSub(field string, subQuery DB) {
	q.addWhereInSub("NOT IN", field, subQuery)
}

func (q *Query) Or() {
	q.wheres = append(q.wheres, where{
		Type:       "logic",
		Comparator: "OR"})
}

func (q *Query) And() {
	q.wheres = append(q.wheres, where{
		Type:       "logic",
		Comparator: "AND"})
}

func (q *Query) OpenBracket() {
	q.wheres = append(q.wheres, where{
		Type:       "bracket",
		Comparator: "("})
}

func (q *Query) CloseBracket() {
	q.wheres = append(q.wheres, where{
		Type:       "bracket",
		Comparator: ")"})
}

func (q *Query) ApplyWheres() (string, []interface{}, []string) {
	whereString := " "
	var params []interface{}
	paramNames := []string{}
	if len(q.wheres) == 0 {
		return whereString, params, paramNames
	}
	first := true
	logic := "AND"
	for i, w := range q.wheres {
		switch w.Type {
		case "where":
			if !first && q.wheres[i-1].Type != "bracket" {
				whereString += fmt.Sprintf(" %s ", logic)
			}
			first = false
			whereString += fmt.Sprintf(" %s %s ", w.Field, w.Comparator)
			if w.Escape {
				if q.useNamedParams {
					whereString += fmt.Sprintf(" @%s ", w.NamedParam)
					paramNames = append(paramNames, w.NamedParam)
				} else {
					whereString += " ? "
				}
				params = append(params, w.Value)

			} else {
				whereString += fmt.Sprintf(" %s ", w.Value)
			}
			if len(w.Params) > 0 {
				params = append(params, w.Params...)
				if q.useNamedParams {
					paramNames = append(paramNames, w.ParamNames...)
				}
			}
		case "logic":
			logic = w.Comparator
		case "bracket":
			if w.Comparator == "(" && !first {
				whereString += fmt.Sprintf(" %s ", logic)
			}
			whereString += fmt.Sprintf(" %s ", w.Comparator)
		}
	}
	return whereString, params, paramNames
}
