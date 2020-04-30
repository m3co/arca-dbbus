package dbbus

import (
	"database/sql"
	"fmt"
	"strings"

	jsonrpc "github.com/m3co/arca-jsonrpc"
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
) (*Result, error) {
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
		return &Result{Success: true, PK: PK}, nil
	}

	if _, err := query.Exec(values...); err != nil {
		if err := tx.Rollback(); err != nil {
			return nil, err
		}
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{Success: true}, nil
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

// Select whatever
func Select(
	db *sql.DB, params map[string]interface{},
	fieldMap map[string]string, table string,
) ([]map[string]interface{}, error) {

	var rows *sql.Rows
	result := []map[string]interface{}{}
	columns := []string{}
	keys := []string{}
	for column := range fieldMap {
		columns = append(columns, fmt.Sprintf(`"%s"`, column))
		keys = append(keys, column)
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
			row[key] = slots[i]
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
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
				params, ok := request.Params.(map[string]interface{})
				if ok {
					fields, pk := getFieldMap()
					return Insert(db, params, fields, pk, table)
				}
				return nil, ErrorMalformedParams
			}
			return nil, ErrorUndefinedParams
		}
	}

	handlers.Delete = func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			if request.Params != nil {
				params, ok := request.Params.(map[string]interface{})
				if ok {
					fields, pk := getFieldMap()
					return Delete(db, params, fields, pk, table)
				}
				return nil, ErrorMalformedParams
			}
			return nil, ErrorUndefinedParams
		}
	}

	handlers.Update = func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			if request.Params != nil {
				params, ok := request.Params.(map[string]interface{})
				if ok {
					fields, keys := getFieldMap()
					return Update(db, params, fields, keys, table)
				}
				return nil, ErrorMalformedParams
			}
			return nil, ErrorUndefinedParams
		}
	}
	return handlers
}

// RegisterSourceIDU whatever
func (server *Server) RegisterSourceIDU(
	source string,
	getFieldMap fieldMap,
	db *sql.DB,
) {
	// IDU(Table) :: Public
	handlers := setupIDU(source, getFieldMap)
	server.RegisterSource("Insert", source, handlers.Insert(db))
	server.RegisterSource("Delete", source, handlers.Delete(db))
	server.RegisterSource("Update", source, handlers.Update(db))
	server.RegisterSource("Select", source, func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			var (
				params map[string]interface{}
				ok     bool
			)
			fields, _ := getFieldMap()

			if request.Params != nil {
				params, ok = request.Params.(map[string]interface{})
				if ok {
				}
			}
			return Select(db, params, fields, source)
		}
	}(db))
}

// RegisterTargetIDU whatever
func (server *Server) RegisterTargetIDU(
	target string,
	getFieldMap fieldMap,
) {
	// idu(_Table) :: Private
	handlers := setupIDU(target, getFieldMap)
	server.RegisterTarget("insert", target, handlers.Insert)
	server.RegisterTarget("delete", target, handlers.Delete)
	server.RegisterTarget("update", target, handlers.Update)
}
