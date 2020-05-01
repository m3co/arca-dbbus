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
				v, err := convert2Numeric(slots[i])
				if v != nil && err == nil {
					row[key] = *v
				} else if err != nil {
					row[key] = err
				} else {
					row[key] = nil
				}
			} else if columnType == "double precision" {
				v := slots[i]
				if v != nil {
					d, ok := v.(float64)
					if ok {
						row[key] = d
					} else {
						v, err := convert2Numeric(slots[i])
						if v != nil && err == nil {
							row[key] = *v
							log.Println("at", table, "turn", key, columnType, "into numeric")
						} else if err != nil {
							row[key] = err
						}
					}
				} else {
					row[key] = nil
				}
			} else if columnType == "integer" {
				v := slots[i]
				if v != nil {
					d, ok := v.(int64)
					if ok {
						row[key] = d
					} else {
						v, err := convert2Numeric(slots[i])
						if v != nil && err == nil {
							row[key] = *v
							log.Println("at", table, "turn", key, columnType, "into numeric")
						} else if err != nil {
							row[key] = err
						}
					}
				} else {
					row[key] = nil
				}
			} else {
				if slots[i] != nil {
					v, ok := slots[i].([]byte)
					if ok {
						row[key] = string(v)
					} else {
						row[key] = slots[i]
					}
				} else {
					row[key] = nil
				}
			}
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
