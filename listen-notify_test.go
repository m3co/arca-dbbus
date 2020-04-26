package dbbus_test

import (
	"net"
	"testing"

	dbbus "github.com/m3co/arca-dbbus"
	jsonrpc "github.com/m3co/arca-jsonrpc"
)

func Test_connect_RegisterDB(t *testing.T) {
	srv := dbbus.Server{}
	defer srv.Close()
	started := make(chan bool)

	connStr, db, err := connect("arca-dbbus-db", "test")
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

	connStr, db, err := connect("arca-dbbus-db", "test")
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
	srv.RegisterSourceIDU("Table1", fieldmap, db)
	srv.RegisterTargetIDU("_Table1", fieldmap)
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
	_, db, conn := singleConn(t, "arca-dbbus-db0")

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-case1"
	request.Method = "Insert"
	request.Context = map[string]string{
		"Source": "Table1",
	}
	row := map[string]string{
		"Field1": "field 1 - case 1 - IDU",
		"Field2": "field 2 - case 1 - IDU",
		"Field3": "field 3 - case 1 - IDU",
		"Field4": "field 4 - case 1 - IDU",
	}
	request.Params = map[string]interface{}{
		"Row": row,
	}

	send(conn, request)
	lastInsertedIDDB0 = 1
	testIfResponseOrNotificationOrWhatever(t, conn, db, row, "insert")
	testIfResponseOrNotificationOrWhatever(t, conn, db, row, "insert")
}

func Test_RegisterIDU_call_Update(t *testing.T) {
	_, db, conn := singleConn(t, "arca-dbbus-db0")

	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	atLeastOneRun := false
	for _, field := range fields {
		if field.ID != lastInsertedIDDB0 {
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
		"Source": "Table1",
	}
	row := map[string]string{
		"Field1": "field 1 - case 1 - IDU update",
		"Field2": "field 2 - case 1 - IDU update",
		"Field3": "field 3 - case 1 - IDU update",
		"Field4": "field 4 - case 1 - IDU update",
	}
	request.Params = map[string]interface{}{
		"Row": row,
		"PK": map[string]int64{
			"ID": lastInsertedIDDB0,
		},
	}

	send(conn, request)
	testIfResponseOrNotificationOrWhatever(t, conn, db, row, "update")
	testIfResponseOrNotificationOrWhatever(t, conn, db, row, "update")
}

func Test_RegisterIDU_call_Delete(t *testing.T) {
	_, db, conn := singleConn(t, "arca-dbbus-db0")

	row := map[string]string{
		"Field1": "field 1 - case 1 - IDU update",
		"Field2": "field 2 - case 1 - IDU update",
		"Field3": "field 3 - case 1 - IDU update",
		"Field4": "field 4 - case 1 - IDU update",
	}
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	atLeastOneRun := false
	for _, field := range fields {
		if field.ID != lastInsertedIDDB0 {
			continue
		}
		if !(*field.Field1 == row["Field1"] &&
			field.Field2 == row["Field2"] &&
			field.Field3 == row["Field3"] &&
			*field.Field4 == row["Field4"]) {
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
		"Source": "Table1",
	}
	request.Params = map[string]interface{}{
		"PK": map[string]int64{
			"ID": lastInsertedIDDB0,
		},
	}

	send(conn, request)
	testIfResponseOrNotificationOrWhatever(t, conn, db, row, "delete")
	testIfResponseOrNotificationOrWhatever(t, conn, db, row, "delete")
}
