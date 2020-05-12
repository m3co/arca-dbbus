package dbbus

import (
	"database/sql"
	"errors"

	"github.com/lib/pq"
	jsonrpc "github.com/m3co/arca-jsonrpc"
)

// Error definitions
var (
	ErrorRPCNotFound          = errors.New("RPC Server not found")
	ErrorZeroParamsInRow      = errors.New("Zero params in Row")
	ErrorZeroParamsInPK       = errors.New("Zero params in PK")
	ErrorZeroParamsInFieldMap = errors.New("Zero params in fieldMap")
	ErrorZeroParamsInKeys     = errors.New("Zero params in keys")
	ErrorUndefinedParams      = errors.New("Params are not defined")
	ErrorMalformedParams      = errors.New("Params is not a map of values")
	ErrorUndefinedRow         = errors.New("Row is not defined")
	ErrorMalformedRow         = errors.New("Row is not a map of values")
	ErrorMalformedPK          = errors.New("PK is not a map of values")
	ErrorUndefinedPK          = errors.New("PK is not defined")
	ErrorUndefinedValuesArray = errors.New("Values array is not defined")
	ErrorEmptyCondition       = errors.New("Condition ended up in empty")
	ErrorIndexNegative        = errors.New("Index cannot be negative")
	ErrorCastToBool           = errors.New("Cannot cast value into boolean")
	ErrorKeyNotInFieldMap     = errors.New("Key is not in the fielMap")
)

// Notification es el mensaje que viene de NOTIFY 'jsonrpc'
type Notification struct {
	// Method = insert | update | delete
	Method string

	// Row contiene la entrada
	Row map[string]interface{}

	// PK contiene los campos "primary-key"
	PK map[string]interface{}

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
	dbs       []*sql.DB
	listeners []*pq.Listener
	rpc       *jsonrpc.Server
	Address   string
}

type handlerIDU struct {
	Insert func(db *sql.DB) jsonrpc.RemoteProcedure
	Delete func(db *sql.DB) jsonrpc.RemoteProcedure
	Update func(db *sql.DB) jsonrpc.RemoteProcedure
}

// Model defines the types in its table, the PK array of its table.
// Also optionally it defines the OrderBy and Limit
type Model struct {
	Row     map[string]string
	PK      []string
	OrderBy string
	Limit   int64
}

type processCell func(value interface{}, row map[string]interface{}, key string) error

// Result shows if a request
type Result struct {
	Success bool
	PK      map[string]interface{} `json:",omitempty"`
}

// ComboboxInfo - deprecated. Do not use.
type ComboboxInfo struct {
	Source  string
	Display string
	Value   string
	Params  map[string]string
}

// FieldInfo - deprecated. Do not use.
type FieldInfo struct {
	Name     string
	Type     string
	Primary  bool
	Required bool
	Editable bool
	Combobox *ComboboxInfo
	Select   *[]string
}

// ActionsInfo - deprecated. Do not use.
type ActionsInfo struct {
	Insert bool
	Delete bool
	Update bool
}

// ModelInfo - deprecated. Do not use.
type ModelInfo struct {
	Actions ActionsInfo
	Fields  []FieldInfo
}
