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
}
type Query struct {
	wheres []where
}

func (q *Query) Where(field string, comparator string, value interface{}, escape bool) {
	w := where{
		Type:       "where",
		Field:      field,
		Comparator: comparator,
		Value:      value,
		Escape:     escape}
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
			valueSlice = append(valueSlice, "?")
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
		Params:     params})
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

func (q *Query) ApplyWheres() (string, []interface{}) {
	whereString := " "
	var params []interface{}
	if len(q.wheres) == 0 {
		return whereString, params
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
				whereString += " ? "
				params = append(params, w.Value)
			} else {
				whereString += fmt.Sprintf(" %s ", w.Value)
			}
			if len(w.Params) > 0 {
				params = append(params, w.Params...)
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
	return whereString, params
}
