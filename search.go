package dbbus

import (
	"database/sql"
	"fmt"
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
) ([]FoundRow, error) {

	searchStr, ok := params["Search"]
	if !ok || searchStr == "" {
		return nil, fmt.Errorf("Search field is required")
	}

	result := []FoundRow{}
	return result, nil
}
