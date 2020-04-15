package dbbus_test

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/joho/godotenv"
	jsonrpc "github.com/m3co/arca-jsonrpc"
)

type ResponseOrNotification struct {
	jsonrpc.Response
	Row map[string]interface{} `json:",omitempty"`
	PK  map[string]interface{} `json:",omitempty"`
}

var (
	connStr  = ""
	fieldMap = map[string]string{
		"ID":     "integer",
		"Field1": "character varying(255)",
		"Field2": "character varying(255)",
		"Field3": "character varying(255)",
		"Field4": "character varying(255)",
	}
	PK                   = []string{"ID"}
	lastInsertedID int64 = 0
)

func fieldmap() (map[string]string, []string) {
	return fieldMap, PK
}

func defineVariables() {
	dbhost := "arca-dbbus-db"
	err := godotenv.Load()
	if err == nil {
		envdbhost := os.Getenv("DB_HOST")
		if envdbhost != "" {
			dbhost = envdbhost
		}
	}
	connStr = fmt.Sprintf("host=%s user=test dbname=test password=test port=5432 sslmode=disable", dbhost)
	fmt.Println(connStr)
}

func init() {
	if connStr == "" {
		defineVariables()
	}
}

// Fields is the struct for the Table
type Fields struct {
	ID             int64
	Field1, Field4 *string
	Field2, Field3 string
}

func connect() (db *sql.DB, err error) {
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return
	}

	err = db.Ping()
	return
}

func selectFieldsFromTable(db *sql.DB) (fields []Fields, err error) {
	var rows *sql.Rows
	fields = []Fields{}
	rows, err = db.Query(`select "ID", "Field1", "Field2", "Field3", "Field4" from "_Table" order by "ID" desc`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var ID int64
		var Field1, Field4 *string
		var Field2, Field3 string
		if err = rows.Scan(&ID, &Field1, &Field2, &Field3, &Field4); err != nil {
			return
		}
		fields = append(fields, Fields{
			ID:     ID,
			Field1: Field1,
			Field2: Field2,
			Field3: Field3,
			Field4: Field4,
		})
	}
	err = rows.Err()
	return
}

func send(conn net.Conn, request *jsonrpc.Request) {
	msg, _ := json.Marshal(request)
	if _, err := conn.Write(msg); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Write([]byte("\n")); err != nil {
		log.Fatal(err)
	}
}

func receive(conn net.Conn) *ResponseOrNotification {
	response := &ResponseOrNotification{}
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	raw := scanner.Bytes()
	if err := json.Unmarshal(raw, response); err != nil {
		log.Fatal(err)
	}
	return response
}
