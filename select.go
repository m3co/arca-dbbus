package dbbus

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

func processNumeric(value interface{}, row map[string]interface{}, key string) error {
	v, err := convert2Numeric(value)
	if v != nil && err == nil {
		row[key] = *v
	} else if err != nil {
		row[key] = err
	} else {
		row[key] = nil
	}
	return err
}

func processDoublePrecision(value interface{}, row map[string]interface{}, key string) error {
	var e error = nil
	if value != nil {
		d, ok := value.(float64)
		if ok {
			row[key] = d
		} else {
			v, err := convert2Numeric(value)
			if v != nil && err == nil {
				row[key] = *v
				e = fmt.Errorf("turn %s into numeric", key)
			} else if err != nil {
				row[key] = err
				e = err
			}
		}
	} else {
		row[key] = nil
	}
	return e
}

func processInteger(value interface{}, row map[string]interface{}, key string) error {
	var e error = nil
	if value != nil {
		d, ok := value.(int64)
		if ok {
			row[key] = d
		} else {
			v, err := convert2Numeric(value)
			if v != nil && err == nil {
				row[key] = *v
				e = fmt.Errorf("turn %s into numeric", key)
			} else if err != nil {
				row[key] = err
				e = err
			}
		}
	} else {
		row[key] = nil
	}
	return e
}

func processOthers(value interface{}, row map[string]interface{}, key string) error {
	if value != nil {
		v, ok := value.([]byte)
		if ok {
			row[key] = string(v)
		} else {
			row[key] = value
		}
	} else {
		row[key] = nil
	}
	return nil
}

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
				processOthers(slots[i], row, key)
			}
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
