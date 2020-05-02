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
	processColumn := []processCell{}
	for column, columnType := range fieldMap {
		tlColumnType := strings.ToLower(columnType)
		columns = append(columns, fmt.Sprintf(`"%s"`, column))
		keys = append(keys, column)
		if strings.Contains(tlColumnType, "numeric") {
			processColumn = append(processColumn, processNumeric)
		} else if tlColumnType == "double precision" {
			processColumn = append(processColumn, processDoublePrecision)
		} else if tlColumnType == "integer" {
			processColumn = append(processColumn, processInteger)
		} else if tlColumnType == "boolean" ||
			tlColumnType == "text" ||
			tlColumnType == "date" ||
			strings.Contains(tlColumnType, "character varying") ||
			strings.Contains(tlColumnType, "timestamp with") ||
			strings.Contains(tlColumnType, "t_") {
			processColumn = append(processColumn, processOther)
		} else {
			return nil, fmt.Errorf("Cannot recognize the type of field %s", columnType)
		}
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
			if err := processColumn[i](slots[i], row, key); err != nil {
				log.Println(err)
			}
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
