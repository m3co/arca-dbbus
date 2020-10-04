package dbbus

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// FoundRow represents the resulting row when performing a search
type FoundRow struct {
	Row   map[string]interface{}
	PK    map[string]interface{}
	Label string
}

// Search makes posible to perform a search in the given table.
// params - contains the dictionary with the folling fields:
//  * Search is an string to search among the fields
//  * Limit falls into the query as limit (optional)
//  * Tag is an string to (if not given then it will use its default tag)
func Search(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, table string,
) ([]map[string]interface{}, error) {

	search, ok := params["Search"]
	if !ok || search == "" {
		return nil, fmt.Errorf("Search field is required")
	}

	values := []interface{}{search}
	result := []map[string]interface{}{}
	columns, keys, processColumn, err := prepareSelectVariables(fieldMap)
	if err != nil {
		return nil, err
	}

	condition := "true"
	if PK, ok := params["PK"]; ok {
		if pk, ok := PK.(map[string]interface{}); ok {
			if condition, err = WherePK(pk, fieldMap, keys, &values, 1); err != nil {
				if err != ErrorZeroParamsInPK {
					return nil, err
				}
			}
		}
	}

	limit := ""
	if Limit, ok := params["Limit"]; ok {
		if l, ok := Limit.(float64); ok {
			li := int64(l)
			limit = fmt.Sprintf("limit %d", li)
		} else {
			return nil, fmt.Errorf("Cannot convert limit param %v into integer", Limit)
		}
	}

	query := fmt.Sprintf(`select %s from "%s" where (%s) and %s %s`,
		strings.Join(columns, ","),
		table,
		searchCondition(search, fieldMap, table), condition,
		limit)
	rows, err := db.Query(query, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	count := len(columns)
	slots := make([]interface{}, count)
	slotsPtrs := make([]interface{}, count)
	for i := range slots {
		slotsPtrs[i] = &slots[i]
	}
	for rows.Next() {
		if err := rows.Scan(slotsPtrs...); err != nil {
			return nil, err
		}
		row := map[string]interface{}{}
		for i, key := range keys {
			if err := processColumn[i](slots[i], row, key); err != nil {
				log.Println(err, "at", table)
			}
		}
		result = append(result, row)
	}
	return result, nil
}

// searchCondition returns the query that perform the search procress among the fields
//  - search.(string) will render in search among fields that can be casted to text
//  - search.(float64) will render in search among fields that can be casted to float
func searchCondition(search interface{}, fieldMap map[string]string, table string) string {
	selectedFields := []string{}
	searchFields := []string{}

	_, isFloat64 := search.(float64)
	_, isString := search.(string)

	for key, value := range fieldMap {
		selectedFields = append(selectedFields, fmt.Sprintf(`"%s"`, key))
		if isString {
			if strings.Contains(value, "bool") {
				continue
			}
			if strings.Contains(value, "numeric") {
				continue
			}
			if strings.Contains(value, "int") {
				continue
			}
			if strings.Contains(value, "time") {
				continue
			}
			if value == "date" {
				continue
			}

			searchFields = append(searchFields,
				fmt.Sprintf(`(lower("%s"::text) like '%%' || lower($1::text) || '%%')`, key))
		} else if isFloat64 {
			skip := true
			if strings.Contains(value, "numeric") {
				skip = false
			}
			if strings.Contains(value, "int") {
				skip = false
			}
			if strings.Contains(value, "float") {
				skip = false
			}
			if strings.Contains(value, "double") {
				skip = false
			}

			if skip {
				continue
			}

			searchFields = append(searchFields,
				fmt.Sprintf(`case when "%s" is null then false else "%s" = $1 end`, key, key))
		}
	}

	return strings.Join(searchFields, " or ")
}
