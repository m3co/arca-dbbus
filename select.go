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
	var (
		rows      *sql.Rows
		condition string
	)
	values := []interface{}{}
	result := []map[string]interface{}{}
	columns, keys, processColumn, err := prepareSelectVariables(fieldMap)
	if err != nil {
		return nil, err
	}

	PK, ok := params["PK"]
	if ok {
		pk, ok := PK.(map[string]interface{})
		if ok {
			if condition, err = WherePK(pk, fieldMap, keys, &values, 0); err != nil {
				if err != ErrorZeroParamsInPK {
					return nil, err
				}
			} else {
				condition = fmt.Sprintf("where %s", condition)
			}
		}
	}

	query := fmt.Sprintf(`select %s from "%s" %s`, strings.Join(columns, ","),
		table, condition)
	rows, err = db.Query(query, values...)
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

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
