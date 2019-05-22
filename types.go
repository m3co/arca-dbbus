package dbbus

import (
	"database/sql"

	jsonrpc "github.com/m3co/arca-jsonrpc"
)

// Notification es el mensaje que viene de NOTIFY 'jsonrpc'
type Notification struct {
	// Method = insert | update | delete
	Method string

	// Row contiene la entrada
	Row interface{}

	// PK contiene los campos "primary-key"
	PK interface{}

	/*
		Context contiene las variables auxiliares del contexto contiene
		 * Source es de que tabla/vista proviene el JSON-RPC
		 * Target es a que tabla/vista está dirigido el JSON-RPC
		 * Db es el nombre de la base de datos
		 * Prime es si Target esta dirigido a una Tabla
		     true  - Target va hacia una tabla "primaria"
				 false - Target va hacia una vista
	*/
	Context map[string]interface{}
}

// Server grid that binds all the DBs with the json-rpc arca server
type Server struct {
	dbs     []*sql.DB
	rpc     *jsonrpc.Server
	Address string
	close   chan bool
}
