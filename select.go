package dbbus

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// Select whatever
func Select(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, table string,
) ([]map[string]interface{}, error) {

	var rows *sql.Rows
	result := []map[string]interface{}{}
	columns := []string{}
	keys := []string{}
	columnTypes := []string{}
	for column, columnType := range fieldMap {
		columns = append(columns, fmt.Sprintf(`"%s"`, column))
		keys = append(keys, column)
		columnTypes = append(columnTypes, columnType)
	}
	count := len(columns)
	slots := make([]interface{}, count)
	slotsPtrs := make([]interface{}, count)
	for i := range slots {
		slotsPtrs[i] = &slots[i]
	}

	query := fmt.Sprintf(`select %s from "%s"`, strings.Join(columns, ","), table)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(slotsPtrs...); err != nil {
			return nil, err
		}
		row := map[string]interface{}{}
		for i, key := range keys {
			columnType := columnTypes[i]
			if strings.Contains(columnType, "numeric") {
				if err := processNumeric(slots[i], row, key); err != nil {
					log.Println("At table", table, err, "it is", columnType)
				}
			} else if columnType == "double precision" {
				if err := processDoublePrecision(slots[i], row, key); err != nil {
					log.Println("At table", table, err, "it is", columnType)
				}
			} else if columnType == "integer" {
				if err := processInteger(slots[i], row, key); err != nil {
					log.Println("At table", table, err, "it is", columnType)
				}
			} else {
				processOther(slots[i], row, key)
			}
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
