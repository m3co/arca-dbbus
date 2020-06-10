package dbbus

import (
	"database/sql"
	"fmt"
	"log"

	jsonrpc "github.com/m3co/arca-jsonrpc"
)

// RegisterSource whatever
func (s *Server) RegisterSource(
	method string, source string, handler jsonrpc.RemoteProcedure) {
	s.rpc.RegisterSource(method, source, handler)
}

// RegisterTarget whatever
func (s *Server) RegisterTarget(
	method string, target string, handler jsonrpc.DBRemoteProcedure) {
	s.rpc.RegisterTarget(method, target, handler)
}

// RegisterDB whatever
func (s *Server) RegisterDB(connStr string, db *sql.DB) error {
	s.dbs = append(s.dbs, db)
	return s.setupListenNotify(connStr)
}

// RegisterSourceIDU whatever
func (s *Server) RegisterSourceIDU(
	source string,
	model *Model,
	db *sql.DB,
) {
	// IDU(Table) :: Public
	handlers := setupIDU(source, model.Row, model.PK)
	s.rpc.RegisterSource("Insert", source, handlers.Insert(db))
	s.rpc.RegisterSource("Delete", source, handlers.Delete(db))
	s.rpc.RegisterSource("Update", source, handlers.Update(db))
	s.rpc.RegisterSource("Select", source, func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			var params map[string]interface{}
			if request.Params != nil {
				Params, ok := request.Params.(map[string]interface{})
				if ok {
					params = Params
				}
			}
			return Select(db, params, model.Row, source, model.OrderBy)
		}
	}(db))
}

// RegisterSourceSearch whatever
func (s *Server) RegisterSourceSearch(
	source string,
	model *Model,
	db *sql.DB,
	labeler interface{},
) {
	tags, ok := labeler.(map[string](func(row map[string]interface{}) (string, error)))
	if !ok {
		fn, ok := labeler.(func(row map[string]interface{}) (string, error))
		if !ok {
			log.Fatal("Cannot cast labeler as a function")
			return
		}
		tags = map[string](func(row map[string]interface{}) (string, error)){}
		tags[""] = fn
	}

	s.rpc.RegisterSource("Search", source, func(db *sql.DB) jsonrpc.RemoteProcedure {
		return func(request *jsonrpc.Request) (interface{}, error) {
			var params map[string]interface{}
			if request.Params == nil {
				return nil, ErrorUndefinedParams
			}
			Params, ok := request.Params.(map[string]interface{})
			if ok {
				params = Params
			} else {
				return nil, ErrorMalformedParams
			}
			rows, err := Search(db, params, model.Row, source)
			if err != nil {
				return nil, err
			}

			tag := ""
			iTag, ok := Params["Tag"]
			if ok {
				tag, ok = iTag.(string)
				if !ok {
					return nil, ErrorMalformedTag
				}
			}
			tagFn, ok := tags[tag]
			if !ok {
				return nil, ErrorUndefinedTag
			}

			results := []FoundRow{}
			for _, row := range rows {
				PK := map[string]interface{}{}
				for _, pk := range model.PK {
					PK[pk] = row[pk]
				}
				label, err := tagFn(row)
				if err != nil {
					return nil, err
				}
				if label == "" {
					continue
				}
				results = append(results, FoundRow{
					PK:    PK,
					Label: label,
				})
			}
			return results, nil
		}
	}(db))
}

// RegisterTargetIDU whatever
func (s *Server) RegisterTargetIDU(
	target string,
	model *Model,
) {
	// idu(_Table) :: Private
	handlers := setupIDU(target, model.Row, model.PK)
	s.rpc.RegisterTarget("insert", target, handlers.Insert)
	s.rpc.RegisterTarget("delete", target, handlers.Delete)
	s.rpc.RegisterTarget("update", target, handlers.Update)
}

// CheckFieldMap test the fieldMap dictionary against the implemented by DB-BUS fields
func (s *Server) CheckFieldMap(model *Model) error {
	Row := model.Row
	PK := model.PK
	_, _, _, err := prepareSelectVariables(Row)
	if err != nil {
		return err
	}
	for _, key := range PK {
		_, ok := Row[key]
		if !ok {
			return fmt.Errorf("Key '%s' not found in the fieldMap", key)
		}
	}
	return err
}

// Close whatever
func (s *Server) Close() error {
	if s.rpc != nil {
		for _, listener := range s.listeners {
			listener.UnlistenAll()
			listener.Close()
		}

		for _, db := range s.dbs {
			db.Close()
		}
		return s.rpc.Close()
	}
	return ErrorRPCNotFound
}

// Start launches the grid server
func (s *Server) Start(started chan bool) error {
	address := ":22345"
	if s.Address == "" {
		s.Address = address
	}
	s.rpc = &jsonrpc.Server{Address: s.Address}
	if err := s.rpc.Start(); err != nil {
		if err1 := s.Close(); err1 != nil {
			log.Println(err1)
		}
		started <- false
		return err
	}

	log.Println("Serving...")
	started <- true
	return nil
}
