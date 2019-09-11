package dbbus

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	jsonrpc "github.com/m3co/arca-jsonrpc"
)

type handlerIDU struct {
	Insert func(db *sql.DB) jsonrpc.RemoteProcedure
	Delete func(db *sql.DB) jsonrpc.RemoteProcedure
	Update func(db *sql.DB) jsonrpc.RemoteProcedure
}

type fieldMap func(params map[string]interface{}) (map[string]string, []string)

// Error definitions
var (
	ErrorZeroParamsInRow = errors.New("Zero params in Row")
	ErrorZeroParamsInPK  = errors.New("Zero params in PK")
	ErrorUndefinedParams = errors.New("Params are not defined")
	ErrorUndefinedRow    = errors.New("Row is not defined")
	ErrorMalformedRow    = errors.New("Row is not a map of values")
	ErrorMalformedPK     = errors.New("PK is not a map of values")
	ErrorUndefinedPK     = errors.New("PK is not defined")
	ErrorEmptyCondition  = errors.New("Condition ended up in empty")
)

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// PrepareAndExecute whatever
func PrepareAndExecute(
	db *sql.DB, queryPrepared string, values ...interface{},
) error {
	tx, err := db.Begin()
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}
		return err
	}

	query, err := tx.Prepare(queryPrepared)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}
		return err
	}
	defer query.Close()

	row, err := query.Exec(values...)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}
		return err
	}

	_, err = row.RowsAffected()
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// Insert whatever
func Insert(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, table string,
) error {
	header := []string{}
	body := []string{}
	values := []interface{}{}
	row, ok := params["Row"]
	if !ok || row == nil {
		return ErrorUndefinedRow
	}
	Row, ok := row.(map[string]interface{})
	if !ok {
		return ErrorMalformedRow
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
		queryPrepared := fmt.Sprintf(`INSERT INTO "%s"(%s) SELECT %s;`,
			table, strings.Join(header, ","), strings.Join(body, ","))

		return PrepareAndExecute(db, queryPrepared, values...)
	}
	return ErrorZeroParamsInRow
}

// Delete whatever
func Delete(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, table string,
) error {
	condition := make([]string, 0)
	values := make([]interface{}, 0)
	var PK map[string]interface{}
	if value, ok := params["PK"]; ok {
		PK, ok = value.(map[string]interface{})
		if !ok {
			return ErrorMalformedPK
		}
		if len(PK) == 0 {
			return ErrorZeroParamsInPK
		}
	} else {
		return ErrorUndefinedPK
	}
	i := 0
	for field, typefield := range fieldMap {
		if value, ok := PK[field]; ok {
			i++
			values = append(values, value)
			condition = append(condition, fmt.Sprintf(`"%s"=$%d::%s`,
				field, i, typefield))
		}
	}
	if len(condition) > 0 {
		queryPrepared := fmt.Sprintf(`delete from "%s" where %s;`,
			table, strings.Join(condition, " and "))

		return PrepareAndExecute(db, queryPrepared, values...)
	}
	return ErrorEmptyCondition
}

// Update whatever
func Update(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, keys []string, table string,
) error {
	body := []string{}
	values := []interface{}{}
	condition := []string{}
	var Row, PK map[string]interface{}
	if value, ok := params["Row"]; ok {
		Row, ok = value.(map[string]interface{})
		if !ok {
			return ErrorMalformedRow
		}
	} else {
		return ErrorUndefinedRow
	}
	i := 0
	for field, typefield := range fieldMap {
		if contains(keys, field) {
			continue
		}
		if value, ok := Row[field]; ok {
			i++
			values = append(values, value)
			body = append(body, fmt.Sprintf(`"%s"=$%d::%s`,
				field, i, typefield))
		}
	}
	if i == 0 {
		return ErrorZeroParamsInRow
	}
	if value, ok := params["PK"]; ok {
		PK, ok = value.(map[string]interface{})
		if !ok {
			return ErrorMalformedPK
		}
		if len(PK) == 0 {
			return ErrorZeroParamsInPK
		}
	} else {
		return ErrorUndefinedPK
	}
	j := 0
	for _, field := range keys {
		if value, ok := PK[field]; ok {
			j++
			values = append(values, value)
			typefield := fieldMap[field]
			condition = append(condition, fmt.Sprintf(`"%s"=$%d::%s`,
				field, i+j, typefield))
		}
	}
	if len(condition) > 0 {
		queryPrepared := fmt.Sprintf(`update "%s" set %s where %s;`,
			table, strings.Join(body, ","), strings.Join(condition, " and "))
		return PrepareAndExecute(db, queryPrepared, values...)
	}
	return ErrorEmptyCondition
}

// setupIDU whatever
func setupIDU(
	table string,
	getFieldMap fieldMap,
) handlerIDU {
	handlers := handlerIDU{}

	handlers.Insert = func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			if request.Params != nil {
				params := request.Params.(map[string]interface{})
				fields, _ := getFieldMap(params)
				return nil, Insert(db, params, fields, table)
			}
			return nil, ErrorUndefinedParams
		}
	}

	handlers.Delete = func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			if request.Params != nil {
				params := request.Params.(map[string]interface{})
				fields, _ := getFieldMap(params)
				return nil, Delete(db, params, fields, table)
			}
			return nil, ErrorUndefinedParams
		}
	}

	handlers.Update = func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			if request.Params != nil {
				params := request.Params.(map[string]interface{})
				fields, keys := getFieldMap(params)
				return nil, Update(db, params, fields, keys, table)
			}
			return nil, ErrorUndefinedParams
		}
	}

	return handlers
}

// RegisterSourceIDU whatever
func RegisterSourceIDU(
	source string,
	getFieldMap fieldMap,
	server *Server,
	db *sql.DB,
) {
	// IDU(Table) :: Public
	handlers := setupIDU(source, getFieldMap)
	server.RegisterSource("Insert", source, handlers.Insert(db))
	server.RegisterSource("Delete", source, handlers.Delete(db))
	server.RegisterSource("Update", source, handlers.Update(db))
}

// RegisterTargetIDU whatever
func RegisterTargetIDU(
	target string,
	getFieldMap fieldMap,
	server *Server,
) {
	// idu(_Table) :: Private
	handlers := setupIDU(target, getFieldMap)
	server.RegisterTarget("insert", target, handlers.Insert)
	server.RegisterTarget("delete", target, handlers.Delete)
	server.RegisterTarget("update", target, handlers.Update)
}
