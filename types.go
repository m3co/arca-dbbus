package dbbus

import (
	"database/sql"
	"errors"

	jsonrpc "github.com/m3co/arca-jsonrpc"
)

var ErrorRPCNotFound = errors.New("RPC Server not found")

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
			* Source es de qué tabla/vista proviene el JSON-RPC
			* Target es a qué tabla/vista está dirigido el JSON-RPC
			* Db es el nombre de la base de datos
			* Prime es si Target esta dirigido a una Tabla primaria
				true  - Target va hacia una tabla "primaria"
				false - Target va hacia una vista
			* Notification es si esta respuesta es tipo notificacion
				true  - Broadcast a todos los interesados
				false - No se indica y no hacer nada
	*/
	Context map[string]interface{}
}

// Server grid that binds all the DBs with the json-rpc arca server
type Server struct {
	dbs     []*sql.DB
	rpc     *jsonrpc.Server
	Address string
}

type handlerIDU struct {
	Insert func(db *sql.DB) jsonrpc.RemoteProcedure
	Delete func(db *sql.DB) jsonrpc.RemoteProcedure
	Update func(db *sql.DB) jsonrpc.RemoteProcedure
}

type fieldMap func() (map[string]string, []string)

// Result shows if a request
type Result struct {
	Success bool
	PK      map[string]interface{} `json:",omitempty"`
}

type ComboboxInfo struct {
	Source  string
	Display string
	Value   string
	Params  map[string]string
}
type FieldInfo struct {
	Name     string
	Type     string
	Primary  bool
	Required bool
	Editable bool
	Combobox *ComboboxInfo
	Select   *[]string
}
type ActionsInfo struct {
	Insert bool
	Delete bool
	Update bool
}
type ModelInfo struct {
	Actions ActionsInfo
	Fields  []FieldInfo
}
