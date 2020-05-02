package dbbus

import (
	"database/sql"
	"fmt"
	"strings"
)

// Insert whatever
func Insert(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, pk []string, table string,
) (*Result, error) {
	header := []string{}
	body := []string{}
	values := []interface{}{}
	row, ok := params["Row"]
	if !ok || row == nil {
		return nil, ErrorUndefinedRow
	}
	Row, ok := row.(map[string]interface{})
	if !ok {
		return nil, ErrorMalformedRow
	}
	i := 0

	for field, typefield := range fieldMap {
		if value, ok := Row[field]; ok {
			i++
			header = append(header, fmt.Sprintf(`"%s"`, field))
			body = append(body, fmt.Sprintf(`$%d::%s as "%s"`, i, typefield, field))
			values = append(values, value)
		}
	}

	if i > 0 {
		queryPrepared := fmt.Sprintf(`insert into "%s"(%s) select %s %s;`,
			table, strings.Join(header, ","), strings.Join(body, ","),
			generateReturning(pk))

		return PrepareAndExecute(db, pk, queryPrepared, values...)
	}
	return nil, ErrorZeroParamsInRow
}

// Delete whatever
func Delete(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, keys []string, table string,
) (*Result, error) {
	values := make([]interface{}, 0)
	var (
		PK        map[string]interface{}
		condition string
		err       error
	)
	if value, ok := params["PK"]; ok {
		PK, ok = value.(map[string]interface{})
		if !ok {
			return nil, ErrorMalformedPK
		}
	} else {
		return nil, ErrorUndefinedPK
	}
	condition, err = WherePK(PK, fieldMap, keys, &values, 0)
	if err != nil {
		return nil, err
	}
	queryPrepared := fmt.Sprintf(`delete from "%s" where %s %s;`,
		table, condition, generateReturning(keys))

	return PrepareAndExecute(db, keys, queryPrepared, values...)
}

// Update whatever
func Update(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, keys []string, table string,
) (*Result, error) {
	body := []string{}
	values := []interface{}{}
	var (
		Row, PK   map[string]interface{}
		condition string
		err       error
	)

	if value, ok := params["Row"]; ok {
		Row, ok = value.(map[string]interface{})
		if !ok {
			return nil, ErrorMalformedRow
		}
	} else {
		return nil, ErrorUndefinedRow
	}
	i := 0
	for field, typefield := range fieldMap {
		if contains(keys, field) {
			vfRow, ok := Row[field]
			if !ok {
				vfRow = nil
			}
			vfPK, ok := PK[field]
			if !ok {
				vfPK = nil
			}
			if (vfPK != nil && vfRow == nil) ||
				(vfPK == nil && vfRow != nil) {

			} else {
				continue
			}
		}
		if value, ok := Row[field]; ok {
			i++
			values = append(values, value)
			body = append(body, fmt.Sprintf(`"%s"=$%d::%s`,
				field, i, typefield))
		}
	}
	if i == 0 {
		return nil, ErrorZeroParamsInRow
	}
	if value, ok := params["PK"]; ok {
		PK, ok = value.(map[string]interface{})
		if !ok {
			return nil, ErrorMalformedPK
		}
	} else {
		return nil, ErrorUndefinedPK
	}
	condition, err = WherePK(PK, fieldMap, keys, &values, i)
	if err != nil {
		return nil, err
	}
	queryPrepared := fmt.Sprintf(`update "%s" set %s where %s %s;`,
		table, strings.Join(body, ","), condition, generateReturning(keys))
	return PrepareAndExecute(db, keys, queryPrepared, values...)
}
