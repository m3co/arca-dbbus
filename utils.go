package dbbus

import (
	"database/sql"
	"fmt"
	"strconv"
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

func convert2Numeric(v interface{}) (res *float64, err error) {
	if v != nil {
		s, ok := v.([]byte)
		if ok {
			c, err := strconv.ParseFloat(string(s), 64)
			if err != nil {
				return nil, err
			}
			res = &c
			err = nil
		} else {
			return nil, fmt.Errorf("Cannot process %v", v)
		}
	}
	return
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

func processOther(value interface{}, row map[string]interface{}, key string) error {
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

// PrepareSelectVariables is a thing for making possible the Select action
func PrepareSelectVariables(fieldMap map[string]string) (columns, keys []string, processColumn []processCell, err error) {
	columns = []string{}
	keys = []string{}
	processColumn = []processCell{}
	for column, columnType := range fieldMap {
		tlColumnType := strings.ToLower(columnType)
		columns = append(columns, fmt.Sprintf(`"%s"`, column))
		keys = append(keys, column)
		if strings.Contains(tlColumnType, "numeric") {
			processColumn = append(processColumn, processNumeric)
		} else if tlColumnType == "double precision" {
			processColumn = append(processColumn, processDoublePrecision)
		} else if tlColumnType == "integer" {
			processColumn = append(processColumn, processInteger)
		} else if tlColumnType == "boolean" ||
			tlColumnType == "text" ||
			tlColumnType == "date" ||
			strings.Contains(tlColumnType, "character varying") ||
			strings.Contains(tlColumnType, "timestamp with") ||
			strings.Contains(tlColumnType, "t_") {
			processColumn = append(processColumn, processOther)
		} else {
			err = fmt.Errorf("Cannot recognize the type of field %s", columnType)
		}
	}
	return
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
