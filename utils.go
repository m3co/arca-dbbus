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

// ErrorZeroParamsInRow indicates that the request cannot be processed
var ErrorZeroParamsInRow = errors.New("Zero params in Row")

// ErrorZeroParamsInPK indicates that the request cannot be processed
var ErrorZeroParamsInPK = errors.New("Zero params in PK")

// ErrorUndefinedParams whatever
var ErrorUndefinedParams = errors.New("Params are not defined")

// ErrorUndefinedRow whatever
var ErrorUndefinedRow = errors.New("Row is not defined")

// ErrorUndefinedPK whatever
var ErrorUndefinedPK = errors.New("PK is not defined")

// ErrorEmptyCondition whatever
var ErrorEmptyCondition = errors.New("Condition ended up in empty")

// ResultOK is the standar result for JSON-RPC-Response Result field
type ResultOK struct {
	Success bool
}

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
) (*ResultOK, error) {
	tx, err := db.Begin()
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return nil, err
		}
		return nil, err
	}

	query, err := tx.Prepare(queryPrepared)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return nil, err
		}
		return nil, err
	}
	defer query.Close()

	row, err := query.Exec(values...)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return nil, err
		}
		return nil, err
	}

	_, err = row.RowsAffected()
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return nil, err
		}
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
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
	Row := params["Row"].(map[string]interface{})
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

		return PrepareAndExecute(db, queryPrepared, values)
	}
	return nil, ErrorZeroParamsInRow
}

// Delete whatever
func Delete(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, table string,
) (interface{}, error) {
	body := make([]string, 0)
	values := make([]interface{}, 0)
	PK := params["PK"].(map[string]interface{})
	i := 0
	for field, typefield := range fieldMap {
		if value, ok := PK[field]; ok {
			i++
			values = append(values, value)
			body = append(body, fmt.Sprintf(`"%s"=$%d::%s`,
				field, i, typefield))
		}
	}
	if i > 0 {
		queryPrepared := fmt.Sprintf(`delete from "%s" where %s;`,
			table, strings.Join(body, " and "))

		return PrepareAndExecute(db, queryPrepared, values)
	}
	return nil, ErrorZeroParamsInPK
}

// Update whatever
func Update(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, keys []string, table string,
) (interface{}, error) {
	body := make([]string, 0)
	values := make([]interface{}, 0)
	condition := make([]string, 0)
	var Row, PK map[string]interface{}
	if value, ok := params["Row"]; ok {
		Row = value.(map[string]interface{})
	} else {
		return nil, ErrorUndefinedRow
	}
	if value, ok := params["PK"]; ok {
		PK = value.(map[string]interface{})
	} else {
		return nil, ErrorUndefinedPK
	}
	i := 0
	for field, typefield := range fieldMap {
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
	if j == 0 {
		return nil, ErrorZeroParamsInPK
	}
	if len(condition) > 0 {
		queryPrepared := fmt.Sprintf(`update "%s" set %s where %s;`,
			table, strings.Join(body, ","), strings.Join(condition, " and "))
		return PrepareAndExecute(db, queryPrepared, values)
	}
	return nil, ErrorEmptyCondition
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
				return Insert(db, params, fields, table)
			}
			return nil, ErrorUndefinedParams
		}
	}

	handlers.Delete = func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			if request.Params != nil {
				params := request.Params.(map[string]interface{})
				fields, _ := getFieldMap(params)
				return Delete(db, params, fields, table)
			}
			return nil, ErrorUndefinedParams
		}
	}

	handlers.Update = func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			if request.Params != nil {
				params := request.Params.(map[string]interface{})
				fields, keys := getFieldMap(params)
				return Update(db, params, fields, keys, table)
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
