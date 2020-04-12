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
func (s *Server) RegisterDB(connStr string, db *sql.DB) {
	s.dbs = append(s.dbs, db)
	go s.setupListenNotify(connStr)
}

// Close whatever
func (s *Server) Close() {
	if s.rpc != nil {
		s.rpc.Close()
	}
	s.close <- true
}

// Start launches the grid server
func (s *Server) Start(started chan bool) error {
	address := ":22345"
	if s.Address != "" {
		address = s.Address
	}
	s.rpc = &jsonrpc.Server{Address: address}
	if err := s.rpc.Start(); err != nil {
		s.Close()
		started <- false
		return err
	}

	log.Println("Serving...")
	started <- true
	<-s.close
	return nil
}
