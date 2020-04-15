package dbbus_test

import (
	"net"
	"testing"
	"time"

	dbbus "github.com/m3co/arca-dbbus"
	jsonrpc "github.com/m3co/arca-jsonrpc"
)

func init() {
	if connStr == "" {
		defineVariables()
	}
}

func Test_connect_RegisterDB(t *testing.T) {
	srv := dbbus.Server{}
	defer srv.Close()
	started := make(chan bool)

	db, err := connect()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer db.Close()
	if err := srv.RegisterDB(connStr, db); err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		t.Fatal("Unexpected error")
	}
}

func Test_call_RegisterIDU(t *testing.T) {
	srv := dbbus.Server{}
	defer srv.Close()
	started := make(chan bool)

	db, err := connect()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer db.Close()

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		t.Fatal("Unexpected error")
	}

	if err := srv.RegisterDB(connStr, db); err != nil {
		t.Fatal(err)
	}
	srv.RegisterSourceIDU("Table", fieldmap, db)
	srv.RegisterTargetIDU("_Table", fieldmap)
}

func Test_call_RegisterIDU_connect(t *testing.T) {
	srv := dbbus.Server{}
	defer srv.Close()
	started := make(chan bool)

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		t.Fatal("Unexpected error")
	}

	conn, err := net.Dial("tcp", srv.Address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
}

func Test_RegisterIDU_call_Insert(t *testing.T) {
	srv := dbbus.Server{}
	defer srv.Close()
	started := make(chan bool)

	db, err := connect()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer db.Close()

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		t.Fatal("Unexpected error")
	}

	if err := srv.RegisterDB(connStr, db); err != nil {
		t.Fatal(err)
	}
	srv.RegisterSourceIDU("Table", fieldmap, db)
	srv.RegisterTargetIDU("_Table", fieldmap)

	conn, err := net.Dial("tcp", srv.Address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-case1"
	request.Method = "Insert"
	request.Context = map[string]string{
		"Source": "Table",
	}
	request.Params = map[string]interface{}{
		"Row": map[string]string{
			"Field1": "field 1 - case 1 - IDU",
			"Field2": "field 2 - case 1 - IDU",
			"Field3": "field 3 - case 1 - IDU",
			"Field4": "field 4 - case 1 - IDU",
		},
	}

	send(conn, request)
	response := receive(conn)
	if response.Error != nil {
		t.Fatal(response.Error.Code, response.Error.Message)
	}
	lastInsertedID++

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
							if !(*field.Field1 == "field 1 - case 1 - IDU" &&
								field.Field2 == "field 2 - case 1 - IDU" &&
								field.Field3 == "field 3 - case 1 - IDU" &&
								*field.Field4 == "field 4 - case 1 - IDU") {
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

	notification := receive(conn)
	context, ok := notification.Context.(map[string]interface{})
	if ok {
		iNotification, ok := context["Notification"]
		if ok {
			isNotifcation, ok := iNotification.(bool)
			if ok {
				if isNotifcation {
					method := notification.Method
					if method != "insert" {
						t.Fatal("notification's method expected as insert")
					}
					row := notification.Row
					field1, ok := row["Field1"]
					if ok {
						sfield1, ok := field1.(string)
						if ok {
							if sfield1 != "field 1 - case 1 - IDU" {
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
							if sfield2 != "field 2 - case 1 - IDU" {
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
							if sfield3 != "field 3 - case 1 - IDU" {
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
							if sfield4 != "field 4 - case 1 - IDU" {
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

func Test_RegisterIDU_call_Update(t *testing.T) {
	srv := dbbus.Server{}
	defer srv.Close()
	started := make(chan bool)

	db, err := connect()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer db.Close()

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		t.Fatal("Unexpected error")
	}

	if err := srv.RegisterDB(connStr, db); err != nil {
		t.Fatal(err)
	}
	srv.RegisterSourceIDU("Table", fieldmap, db)
	srv.RegisterTargetIDU("_Table", fieldmap)

	conn, err := net.Dial("tcp", srv.Address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	atLeastOneRun := false
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(*field.Field1 == "field 1 - case 1 - IDU" &&
			field.Field2 == "field 2 - case 1 - IDU" &&
			field.Field3 == "field 3 - case 1 - IDU" &&
			*field.Field4 == "field 4 - case 1 - IDU") {
			t.Fatal("Unexpected row at case 1 - update")
		}
		atLeastOneRun = true
	}
	if atLeastOneRun == false {
		t.Fatal("Nothing was tested at Test_RegisterIDU_call_Update - take 1")
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-case1"
	request.Method = "Update"
	request.Context = map[string]string{
		"Source": "Table",
	}
	request.Params = map[string]interface{}{
		"Row": map[string]string{
			"Field1": "field 1 - case 1 - IDU update",
			"Field2": "field 2 - case 1 - IDU update",
			"Field3": "field 3 - case 1 - IDU update",
			"Field4": "field 4 - case 1 - IDU update",
		},
		"PK": map[string]int64{
			"ID": lastInsertedID,
		},
	}

	send(conn, request)
	response := receive(conn)
	if response.Error != nil {
		t.Fatal(response.Error.Code, response.Error.Message)
	}

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
							if !(*field.Field1 == "field 1 - case 1 - IDU update" &&
								field.Field2 == "field 2 - case 1 - IDU update" &&
								field.Field3 == "field 3 - case 1 - IDU update" &&
								*field.Field4 == "field 4 - case 1 - IDU update") {
								t.Fatal("Unexpected row at case 1 when updating")
							}
							atLeastOneRun = true
						}
						if atLeastOneRun == false {
							t.Fatal("Nothing was tested at Test_RegisterIDU_call_Update - take 2")
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

	notification := receive(conn)
	context, ok := notification.Context.(map[string]interface{})
	if ok {
		iNotification, ok := context["Notification"]
		if ok {
			isNotifcation, ok := iNotification.(bool)
			if ok {
				if isNotifcation {
					method := notification.Method
					if method != "update" {
						t.Fatal("notification's method expected as insert")
					}
					row := notification.Row
					field1, ok := row["Field1"]
					if ok {
						sfield1, ok := field1.(string)
						if ok {
							if sfield1 != "field 1 - case 1 - IDU update" {
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
							if sfield2 != "field 2 - case 1 - IDU update" {
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
							if sfield3 != "field 3 - case 1 - IDU update" {
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
							if sfield4 != "field 4 - case 1 - IDU update" {
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

func Test_RegisterIDU_call_Delete(t *testing.T) {
	srv := dbbus.Server{}
	defer srv.Close()
	started := make(chan bool)

	db, err := connect()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer db.Close()

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		t.Fatal("Unexpected error")
	}

	if err := srv.RegisterDB(connStr, db); err != nil {
		t.Fatal(err)
	}
	srv.RegisterSourceIDU("Table", fieldmap, db)
	srv.RegisterTargetIDU("_Table", fieldmap)

	conn, err := net.Dial("tcp", srv.Address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	atLeastOneRun := false
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(*field.Field1 == "field 1 - case 1 - IDU update" &&
			field.Field2 == "field 2 - case 1 - IDU update" &&
			field.Field3 == "field 3 - case 1 - IDU update" &&
			*field.Field4 == "field 4 - case 1 - IDU update") {
			t.Fatal("Unexpected row at case 1 - delete")
		}
		atLeastOneRun = true
	}
	if atLeastOneRun == false {
		t.Fatal("Nothing was tested at Test_RegisterIDU_call_Delete")
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-case1"
	request.Method = "Delete"
	request.Context = map[string]string{
		"Source": "Table",
	}
	request.Params = map[string]interface{}{
		"PK": map[string]int64{
			"ID": lastInsertedID,
		},
	}

	send(conn, request)
	response := receive(conn)
	if response.Error != nil {
		t.Fatal(response.Error.Code, response.Error.Message)
	}

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
							atLeastOneRun = true
						}
						if atLeastOneRun == true {
							t.Fatal("Expecting Nothing")
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

	notification := receive(conn)
	context, ok := notification.Context.(map[string]interface{})
	if ok {
		iNotification, ok := context["Notification"]
		if ok {
			isNotifcation, ok := iNotification.(bool)
			if ok {
				if isNotifcation {
					method := notification.Method
					if method != "delete" {
						t.Fatal("notification's method expected as insert")
					}
					row := notification.Row
					field1, ok := row["Field1"]
					if ok {
						sfield1, ok := field1.(string)
						if ok {
							if sfield1 != "field 1 - case 1 - IDU update" {
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
							if sfield2 != "field 2 - case 1 - IDU update" {
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
							if sfield3 != "field 3 - case 1 - IDU update" {
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
							if sfield4 != "field 4 - case 1 - IDU update" {
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
