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
	db *sql.DB, pk []string, queryPrepared string, values ...interface{},
) (interface{}, error) {
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

	if pk != nil {
		row := query.QueryRow(values...)

		ret := make([]interface{}, len(pk))
		retL := make([]*interface{}, len(pk))
		for i := range pk {
			var v interface{}
			retL[i] = &v
			ret[i] = &v
		}

		if err := row.Scan(ret...); err != nil && err != sql.ErrNoRows {
			if err := tx.Rollback(); err != nil {
				return nil, err
			}
			return nil, err
		}

		if err := tx.Commit(); err != nil {
			return nil, err
		}

		PK := map[string]interface{}{}
		for i, key := range pk {
			PK[key] = *retL[i]
		}
		return PK, nil
	}

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
	return row, nil
}

func generateReturning(pk []string) string {
	var pks string
	if pk != nil {
		pkM := make([]string, len(pk))
		for i, v := range pk {
			pkM[i] = fmt.Sprintf(`"%s"`, v)
		}
		pks = fmt.Sprintf("returning %s", strings.Join(pkM, ","))
	}
	return pks
}

// Insert whatever
func Insert(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, pk []string, table string,
) (interface{}, error) {
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
	fieldMap map[string]string, pk []string, table string,
) (interface{}, error) {
	condition := make([]string, 0)
	values := make([]interface{}, 0)
	var PK map[string]interface{}
	if value, ok := params["PK"]; ok {
		PK, ok = value.(map[string]interface{})
		if !ok {
			return nil, ErrorMalformedPK
		}
		if len(PK) == 0 {
			return nil, ErrorZeroParamsInPK
		}
	} else {
		return nil, ErrorUndefinedPK
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
		queryPrepared := fmt.Sprintf(`delete from "%s" where %s %s;`,
			table, strings.Join(condition, " and "),
			generateReturning(pk))

		return PrepareAndExecute(db, pk, queryPrepared, values...)
	}
	return nil, ErrorEmptyCondition
}

// Update whatever
func Update(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, keys []string, table string,
) (interface{}, error) {
	body := []string{}
	values := []interface{}{}
	condition := []string{}
	var Row, PK map[string]interface{}
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
		if len(PK) == 0 {
			return nil, ErrorZeroParamsInPK
		}
	} else {
		return nil, ErrorUndefinedPK
	}
	j := 0
	for _, field := range keys {
		if value, ok := PK[field]; ok {
			if value != nil {
				j++
				values = append(values, value)
				typefield := fieldMap[field]
				condition = append(condition, fmt.Sprintf(`"%s"=$%d::%s`,
					field, i+j, typefield))
			} else {
				condition = append(condition, fmt.Sprintf(`"%s" is null`,
					field))
			}
		}
	}
	if len(condition) > 0 {
		queryPrepared := fmt.Sprintf(`update "%s" set %s where %s %s;`,
			table, strings.Join(body, ","), strings.Join(condition, " and "),
			generateReturning(keys))
		return PrepareAndExecute(db, keys, queryPrepared, values...)
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
				fields, pk := getFieldMap(params)
				return Insert(db, params, fields, pk, table)
			}
			return nil, ErrorUndefinedParams
		}
	}

	handlers.Delete = func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			if request.Params != nil {
				params := request.Params.(map[string]interface{})
				fields, pk := getFieldMap(params)
				return Delete(db, params, fields, pk, table)
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
