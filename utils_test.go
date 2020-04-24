package dbbus_test

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

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
	PK                      = []string{"ID"}
	lastInsertedID    int64 = 0
	lastInsertedIDDB0 int64 = 0
	srvDb0            *dbbus.Server
	dbDb0             *sql.DB
	connDb0           net.Conn
)

func fieldmap() (map[string]string, []string) {
	return fieldMap, PK
}

func singleConn(t *testing.T, currdb string) (srv *dbbus.Server, db *sql.DB, conn net.Conn) {
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

	connStr, db, err = connect(currdb, "test")
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
	srv.RegisterSourceIDU("Table1", fieldmap, db)
	srv.RegisterTargetIDU("_Table1", fieldmap)

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
						if response.Method == "Delete" {
							atLeastOneRun := false
							for _, field := range fields {
								if field.ID != lastInsertedIDDB0 {
									continue
								}
								atLeastOneRun = true
							}
							if atLeastOneRun == true {
								t.Fatal("Expecting Nothing")
							}
						} else {
							atLeastOneRun := false
							for _, field := range fields {
								if field.ID != lastInsertedIDDB0 {
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
								t.Fatal("Nothing was tested at Test_RegisterIDU_call_", response.Method)
							}
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

func testIfResponseOrNotificationOrWhatever(t *testing.T, conn net.Conn, db *sql.DB, row map[string]string, method string) {
	msg := receive(conn)
	if msg.ID != "" {
		response := msg
		if response.Error != nil {
			t.Fatal(response.Error.Code, response.Error.Message)
		}
		checkResponse(t, response, db, row)
	} else {
		context, ok := msg.Context.(map[string]interface{})
		if ok {
			_, ok := context["Notification"]
			if ok {
				notification := msg
				checkNotification(t, notification, row, method)
			}
		} else {
			t.Fatal("Unexpected response", msg)
		}
	}
}

func makeConnStr(host, db string) string {
	res := fmt.Sprintf("user=test dbname=%s password=test port=5432 sslmode=disable host=%s", db, host)
	return res
}

// Fields is the struct for the Table
type Fields struct {
	ID             int64
	Field1, Field4 *string
	Field2, Field3 string
}

func connect(host, dbname string) (conn string, db *sql.DB, err error) {
	conn = makeConnStr(host, dbname)
	db, err = sql.Open("postgres", conn)
	if err != nil {
		log.Fatal(err)
		return
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	return
}

func selectFieldsFromTable(db *sql.DB) (fields []Fields, err error) {
	var rows *sql.Rows
	fields = []Fields{}
	rows, err = db.Query(`select "ID", "Field1", "Field2", "Field3", "Field4" from "_Table1" order by "ID" desc`)
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

func createSwarm(t *testing.T) (*dbbus.Server, *sql.DB, *sql.DB, *sql.DB, *sql.DB) {

	connDBMaster, dbMaster, err := connect("arca-dbbus-db-master", "test-master")
	if err != nil {
		dbMaster.Close()
		t.Fatal(err)
		return nil, nil, nil, nil, nil
	}

	connDBView12, dbView12, err := connect("arca-dbbus-db-view12", "test-view12")
	if err != nil {
		dbMaster.Close()
		dbView12.Close()
		t.Fatal(err)
		return nil, nil, nil, nil, nil
	}

	connDBView23, dbView23, err := connect("arca-dbbus-db-view23", "test-view23")
	if err != nil {
		dbMaster.Close()
		dbView12.Close()
		dbView23.Close()
		t.Fatal(err)
		return nil, nil, nil, nil, nil
	}

	connDBView123, dbView123, err := connect("arca-dbbus-db-view123", "test-view123")
	if err != nil {
		dbMaster.Close()
		dbView12.Close()
		dbView23.Close()
		dbView123.Close()
		t.Fatal(err)
		return nil, nil, nil, nil, nil
	}

	srv := &dbbus.Server{Address: ":22346"}
	started := make(chan bool)

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		dbMaster.Close()
		dbView12.Close()
		dbView23.Close()
		dbView123.Close()
		srv.Close()
		t.Fatal("Unexpected error")
		return nil, nil, nil, nil, nil
	}

	if err := srv.RegisterDB(connDBMaster, dbMaster); err != nil {
		dbMaster.Close()
		dbView12.Close()
		dbView23.Close()
		dbView123.Close()
		t.Fatal(err)
		return nil, nil, nil, nil, nil
	}
	if err := srv.RegisterDB(connDBView12, dbView12); err != nil {
		dbMaster.Close()
		dbView12.Close()
		dbView23.Close()
		dbView123.Close()
		t.Fatal(err)
		return nil, nil, nil, nil, nil
	}
	if err := srv.RegisterDB(connDBView23, dbView23); err != nil {
		dbMaster.Close()
		dbView12.Close()
		dbView23.Close()
		dbView123.Close()
		t.Fatal(err)
		return nil, nil, nil, nil, nil
	}
	if err := srv.RegisterDB(connDBView123, dbView123); err != nil {
		dbMaster.Close()
		dbView12.Close()
		dbView23.Close()
		dbView123.Close()
		t.Fatal(err)
		return nil, nil, nil, nil, nil
	}

	return srv, dbMaster, dbView12, dbView23, dbView123
}

// Fields is the struct for the Table
type Table1Fields struct {
	ID             int64
	Field1, Field4 *string
	Field2, Field3 string
}

var (
	errorField1Unexpected = errors.New("Field1 unexpected")
	errorField1NotOnlyOne = errors.New("Expected only one result")
)

func checkFromTable1(t *testing.T, db *sql.DB, ID int64, row map[string]string) ([]Table1Fields, error) {
	var rows *sql.Rows
	fields := []Table1Fields{}
	rows, err := db.Query(fmt.Sprintf(`select "ID", "Field1", "Field2", "Field3", "Field4" from "_Table1" where "ID" = %d`, ID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ID int64
		var Field1, Field4 *string
		var Field2, Field3 string
		if err := rows.Scan(&ID, &Field1, &Field2, &Field3, &Field4); err != nil {
			t.Fatal(err)
		}
		fields = append(fields, Table1Fields{
			ID:     ID,
			Field1: Field1,
			Field2: Field2,
			Field3: Field3,
			Field4: Field4,
		})
	}
	if err := rows.Err(); err != nil {
		return fields, err
	}

	if len(fields) == 1 {
		field := fields[0]
		if *field.Field1 == row["Field1"] &&
			field.Field2 == row["Field2"] &&
			field.Field3 == row["Field3"] &&
			*field.Field4 == row["Field4"] {
			return fields, nil
		} else {
			return fields, errorField1Unexpected
		}
	} else {
		return fields, errorField1NotOnlyOne
	}
}
