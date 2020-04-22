package dbbus_test

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	dbbus "github.com/m3co/arca-dbbus"
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
	srvDb0         *dbbus.Server
	dbDb0          *sql.DB
	connDb0        net.Conn
)

func fieldmap() (map[string]string, []string) {
	return fieldMap, PK
}

func singleConn(t *testing.T) (srv *dbbus.Server, db *sql.DB, conn net.Conn) {
	if srvDb0 != nil && dbDb0 != nil && connDb0 != nil {
		srv = srvDb0
		db = dbDb0
		conn = connDb0
		return
	}

	var err error
	srv = &dbbus.Server{}
	srvDb0 = srv
	started := make(chan bool)

	db, err = connect()
	if err != nil {
		t.Fatal(err)
		srv.Close()
		db.Close()
		srvDb0 = nil
		return
	}
	dbDb0 = db

	go func() {
		if err = srv.Start(started); err != nil {
			srv.Close()
			db.Close()
			srvDb0 = nil
			dbDb0 = nil
			t.Error(err)
		}
	}()

	if <-started != true {
		t.Fatal("Unexpected error")
	}

	if err = srv.RegisterDB(connStr, db); err != nil {
		srv.Close()
		db.Close()
		srvDb0 = nil
		dbDb0 = nil
		t.Fatal(err)
		return
	}
	srv.RegisterSourceIDU("Table", fieldmap, db)
	srv.RegisterTargetIDU("_Table", fieldmap)

	conn, err = net.Dial("tcp", srv.Address)
	if err != nil {
		srv.Close()
		db.Close()
		conn.Close()
		srvDb0 = nil
		dbDb0 = nil
		connDb0 = nil
		t.Fatal(err)
		return
	}
	connDb0 = conn
	return
}

func checkResponse(t *testing.T, response *ResponseOrNotification, db *sql.DB, expectedField map[string]string) {
	msg, _ := json.Marshal(response)
	t.Log("Response", string(msg))
	// the following is a hell but I won't care
	if successAndPK, ok := response.Result.(map[string]interface{}); ok {
		if success, ok := successAndPK["Success"].(bool); ok {
			if PK, ok := successAndPK["PK"].(map[string]interface{}); ok {
				if ID, ok := PK["ID"].(float64); ok {
					if success && ID > 0 {
						time.Sleep(100 * time.Millisecond) // let's wait for the DB to complete the transaction
						fields, err := selectFieldsFromTable(db)
						if err != nil {
							t.Fatal(err)
						}
						atLeastOneRun := false
						for _, field := range fields {
							if field.ID != lastInsertedID {
								continue
							}
							if !(*field.Field1 == expectedField["Field1"] &&
								field.Field2 == expectedField["Field2"] &&
								field.Field3 == expectedField["Field3"] &&
								*field.Field4 == expectedField["Field4"]) {
								t.Fatal("Unexpected row at case 1 when inserting")
							}
							atLeastOneRun = true
						}
						if atLeastOneRun == false {
							t.Fatal("Nothing was tested at Test_RegisterIDU_call_Insert")
						}
					} else {
						t.Fatal("unexpected result")
					}
				} else {
					t.Fatal(`PK["ID"].(float64) error`)
				}
			} else {
				t.Fatal(`successAndPK["PK"].(map[string]interface{}) error`)
			}
		} else {
			t.Fatal(`successAndPK["Success"].(bool) error`)
		}
	} else {
		t.Fatal("response.Result.(map[string]interface{}) error")
	}
}

func checkNotification(t *testing.T, notification *ResponseOrNotification, expectedField map[string]string, method string) {
	msg, _ := json.Marshal(notification)
	t.Log("Notification", string(msg))
	context, ok := notification.Context.(map[string]interface{})
	if ok {
		iNotification, ok := context["Notification"]
		if ok {
			isNotifcation, ok := iNotification.(bool)
			if ok {
				if isNotifcation {
					method := notification.Method
					if method != method {
						t.Fatal("notification's method expected as insert")
					}
					row := notification.Row
					field1, ok := row["Field1"]
					if ok {
						sfield1, ok := field1.(string)
						if ok {
							if sfield1 != expectedField["Field1"] {
								t.Fatal("field1 unexpected")
							}
						} else {
							t.Fatal("field1.(string) error")
						}
					}
					field2, ok := row["Field2"]
					if ok {
						sfield2, ok := field2.(string)
						if ok {
							if sfield2 != expectedField["Field2"] {
								t.Fatal("field2 unexpected")
							}
						} else {
							t.Fatal("field2.(string) error")
						}
					}
					field3, ok := row["Field3"]
					if ok {
						sfield3, ok := field3.(string)
						if ok {
							if sfield3 != expectedField["Field3"] {
								t.Fatal("field3 unexpected")
							}
						} else {
							t.Fatal("field3.(string) error")
						}
					}
					field4, ok := row["Field4"]
					if ok {
						sfield4, ok := field4.(string)
						if ok {
							if sfield4 != expectedField["Field4"] {
								t.Fatal("field4 unexpected")
							}
						} else {
							t.Fatal("field4.(string) error")
						}
					}
				} else {
					t.Fatal("received notification is not a notification error")
				}
			} else {
				t.Fatal("iNotification.(bool) error")
			}
		} else {
			t.Fatal(`context has no Notification field, error`)
		}
	} else {
		t.Fatal("notification.Context.(map[string]interface{}) error")
	}
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
