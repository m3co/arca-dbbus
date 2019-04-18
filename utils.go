package dbbus

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ErrorZeroParams indicates that the request cannot be processed
var ErrorZeroParams = errors.New("Zero params")

// ResultOK is the standar result for JSON-RPC-Response Result field
type ResultOK struct {
	Success bool
}

func prepareAndExecute(
	db *sql.DB, queryPrepared string, values []interface{},
) (*ResultOK, error) {
	tx, err := db.Begin()
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	query, err := tx.Prepare(queryPrepared)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer query.Close()

	if err != nil {
		tx.Rollback()
		return nil, err
	}

	row, err := query.Exec(values...)

	if err != nil {
		tx.Rollback()
		return nil, err
	}

	_, err = row.RowsAffected()
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	tx.Commit()
	return &ResultOK{true}, nil
}

// Insert whatever
func Insert(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, table string,
) (interface{}, error) {
	header := make([]string, 0)
	body := make([]string, 0)
	values := make([]interface{}, 0)
	i := 0

	for field, typefield := range fieldMap {
		i++
		header = append(header, fmt.Sprintf(`"%s"`, field))
		body = append(body, fmt.Sprintf(`$%d::%s as "%s"`, i, typefield, field))
		values = append(values, params[field])
	}

	if i > 0 {
		queryPrepared := fmt.Sprintf(`INSERT INTO "%s"(%s) SELECT %s;`,
			table, strings.Join(header, ","), strings.Join(body, ","))

		return prepareAndExecute(db, queryPrepared, values)
	}
	return nil, ErrorZeroParams
}

// Delete whatever
func Delete(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, table string,
) (interface{}, error) {
	body := make([]string, 0)
	values := make([]interface{}, 0)
	i := 0
	for field, typefield := range fieldMap {
		i++
		values = append(values, params[field])
		body = append(body, fmt.Sprintf(`"%s"=$%d::%s`,
			field, i, typefield))
	}
	if i > 0 {
		queryPrepared := fmt.Sprintf(`delete from "%s" where %s;`,
			table, strings.Join(body, " and "))

		return prepareAndExecute(db, queryPrepared, values)
	}
	return nil, ErrorZeroParams
}

// Update whatever
func Update(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, table string,
) (interface{}, error) {
	body := make([]string, 0)
	values := make([]interface{}, 0)
	condition := ""
	i := 0
	for field, typefield := range fieldMap {
		i++
		values = append(values, params[field])
		if field == "ID" {
			condition = fmt.Sprintf(`where "ID"=$%d::%s`, i, typefield)
			continue
		}
		body = append(body, fmt.Sprintf(`"%s"=$%d::%s`,
			field, i, typefield))
	}
	if condition != "" {
		queryPrepared := fmt.Sprintf(`update "%s" set %s %s;`,
			table, strings.Join(body, ","), condition)

		return prepareAndExecute(db, queryPrepared, values)
	}
	return nil, ErrorZeroParams
}
