package dbbus

import (
	"database/sql"
	"fmt"
	"strings"
)

// FoundRow represents the resulting row when performing a search
type FoundRow struct {
	Value string
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

	query := searchCreateQuery(search, fieldMap, table)
	fmt.Println(query, search)

	result := []map[string]interface{}{}
	return result, nil
}

// searchCreateQuery returns the query that perform the search procress among the fields
//  - search.(string) will render in search among fields that can be casted to text
//  - search.(float64) will render in search among fields that can be casted to float
func searchCreateQuery(search interface{}, fieldMap map[string]string, table string) string {
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
				fmt.Sprintf(`("%s"::float = $1::float`, key))
		}
	}

	return fmt.Sprintf("select %s from %s where %s",
		strings.Join(selectedFields, ","),
		table,
		strings.Join(searchFields, " or "),
	)
}
