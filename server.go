package dbbus

import (
	"database/sql"
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
	getFieldMap fieldMap,
	db *sql.DB,
) {
	// IDU(Table) :: Public
	handlers := setupIDU(source, getFieldMap)
	s.rpc.RegisterSource("Insert", source, handlers.Insert(db))
	s.rpc.RegisterSource("Delete", source, handlers.Delete(db))
	s.rpc.RegisterSource("Update", source, handlers.Update(db))
	s.rpc.RegisterSource("Select", source, func(db *sql.DB) jsonrpc.RemoteProcedure {
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
func (s *Server) RegisterTargetIDU(
	target string,
	getFieldMap fieldMap,
) {
	// idu(_Table) :: Private
	handlers := setupIDU(target, getFieldMap)
	s.rpc.RegisterTarget("insert", target, handlers.Insert)
	s.rpc.RegisterTarget("delete", target, handlers.Delete)
	s.rpc.RegisterTarget("update", target, handlers.Update)
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
