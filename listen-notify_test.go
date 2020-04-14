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
}
