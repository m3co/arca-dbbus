package dbbus

import (
	"database/sql"

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
func (s *Server) RegisterDB(connStr string, connect func() *sql.DB) *sql.DB {
	db := connect()
	s.dbs = append(s.dbs, db)
	go s.setupListenNotify(connStr)
	return db
}

// Close whatever
func (s *Server) Close() {
	if s.rpc != nil {
		s.rpc.Close()
	}
	s.close <- true
}

// Start launches the grid server
func (s *Server) Start(ready *chan bool) (err error) {
	//s.dbs = make([]*sql.DB)
	address := ":12345"
	if s.Address != "" {
		address = s.Address
	}
	s.rpc = &jsonrpc.Server{Address: address}

	readyJSONRPC := make(chan bool)

	go (func() {
		err := s.rpc.Start(&readyJSONRPC)
		if err != nil {
			s.Close()
			panic(err)
		}
	})()

	<-readyJSONRPC

	println("Serving...")

	*ready <- true
	<-s.close
	return
}
