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

// ErrorZeroParams indicates that the request cannot be processed
var ErrorZeroParams = errors.New("Zero params")

// ErrorUndefinedParams whatever
var ErrorUndefinedParams = errors.New("Params are not defined")

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

// setupIDU whatever
func setupIDU(
	table string,
	getFieldMap func(params map[string]interface{}) map[string]string,
) handlerIDU {
	handlers := handlerIDU{}

	handlers.Insert = func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			if request.Params != nil {
				params := request.Params.(map[string]interface{})
				fields := getFieldMap(params)
				return Insert(db, params, fields, table)
			}
			return nil, ErrorUndefinedParams
		}
	}

	handlers.Delete = func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			if request.Params != nil {
				params := request.Params.(map[string]interface{})
				fields := getFieldMap(params)
				return Delete(db, params, fields, table)
			}
			return nil, ErrorUndefinedParams
		}
	}

	handlers.Update = func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			if request.Params != nil {
				params := request.Params.(map[string]interface{})
				fields := getFieldMap(params)
				return Update(db, params, fields, table)
			}
			return nil, ErrorUndefinedParams
		}
	}

	return handlers
}

// RegisterSourceIDU whatever
func RegisterSourceIDU(
	source string,
	getFieldMap func(params map[string]interface{}) map[string]string,
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
	getFieldMap func(params map[string]interface{}) map[string]string,
	server *Server,
) {
	// idu(_Table) :: Private
	handlers := setupIDU(target, getFieldMap)
	server.RegisterTarget("insert", target, handlers.Insert)
	server.RegisterTarget("delete", target, handlers.Delete)
	server.RegisterTarget("update", target, handlers.Update)
}
