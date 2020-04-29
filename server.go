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
