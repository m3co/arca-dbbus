package dbbus_test

import (
	"net"
	"testing"
	"time"

	jsonrpc "github.com/m3co/arca-jsonrpc"
)

func Table1Map() (map[string]string, []string) {
	return map[string]string{
		"ID":     "integer",
		"Field1": "character varying(255)",
		"Field2": "character varying(255)",
		"Field3": "character varying(255)",
		"Field4": "character varying(255)",
	}, []string{"ID"}
}

func Table2Map() (map[string]string, []string) {
	return map[string]string{
		"ID":     "integer",
		"Field5": "character varying(255)",
		"Field6": "character varying(255)",
		"Field7": "character varying(255)",
		"Field8": "character varying(255)",
	}, []string{"ID"}
}

func Table3Map() (map[string]string, []string) {
	return map[string]string{
		"ID":      "integer",
		"Field9":  "character varying(255)",
		"Field10": "character varying(255)",
		"Field11": "character varying(255)",
		"Field12": "character varying(255)",
	}, []string{"ID"}
}

func Table1Table2Map() (map[string]string, []string) {
	return map[string]string{
		"ID1-ID2": "text",
		"Field1":  "character varying(255)",
		"Field2":  "character varying(255)",
		"Field3":  "character varying(255)",
		"Field4":  "character varying(255)",
		"Field5":  "character varying(255)",
		"Field6":  "character varying(255)",
		"Field7":  "character varying(255)",
		"Field8":  "character varying(255)",
	}, []string{"ID1-ID2"}
}

func Table2Table3Map() (map[string]string, []string) {
	return map[string]string{
		"ID2-ID3": "text",
		"Field5":  "character varying(255)",
		"Field6":  "character varying(255)",
		"Field7":  "character varying(255)",
		"Field8":  "character varying(255)",
		"Field9":  "character varying(255)",
		"Field10": "character varying(255)",
		"Field11": "character varying(255)",
		"Field12": "character varying(255)",
	}, []string{"ID2-ID3"}
}

func Table1Table2Table3Map() (map[string]string, []string) {
	return map[string]string{
		"ID1-ID2-ID3": "text",
		"Field1":      "character varying(255)",
		"Field2":      "character varying(255)",
		"Field3":      "character varying(255)",
		"Field4":      "character varying(255)",
		"Field5":      "character varying(255)",
		"Field6":      "character varying(255)",
		"Field7":      "character varying(255)",
		"Field8":      "character varying(255)",
		"Field9":      "character varying(255)",
		"Field10":     "character varying(255)",
		"Field11":     "character varying(255)",
		"Field12":     "character varying(255)",
	}, []string{"ID1-ID2-ID3"}
}

func Test_check_allDBs(t *testing.T) {
	srv, dbMaster, dbView12, dbView23, dbView123 := createSwarm(t)
	srv.Close()
	dbMaster.Close()
	dbView12.Close()
	dbView23.Close()
	dbView123.Close()

	time.Sleep(500 * time.Millisecond)
}

func Test_DBMaster_Table1_Insert(t *testing.T) {
	srv, dbMaster, dbView12, dbView23, dbView123 := createSwarm(t)

	srv.RegisterSourceIDU("Table1", Table1Map, dbMaster)
	srv.RegisterTargetIDU("_Table1", Table1Map)

	srv.RegisterSourceIDU("Table2", Table2Map, dbMaster)
	srv.RegisterTargetIDU("_Table2", Table2Map)

	srv.RegisterSourceIDU("Table3", Table3Map, dbMaster)
	srv.RegisterTargetIDU("_Table3", Table3Map)

	srv.RegisterSourceIDU("Table1-Table2", Table1Table2Map, dbView12)
	srv.RegisterSourceIDU("Table2-Table3", Table2Table3Map, dbView23)

	srv.RegisterSourceIDU("Table1-Table2-Table3", Table1Table2Table3Map, dbView123)

	conn, err := net.Dial("tcp", srv.Address)
	if err != nil {
		srv.Close()
		dbMaster.Close()
		dbView12.Close()
		dbView23.Close()
		dbView123.Close()
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-complex-case-insert"
	request.Method = "Insert"
	request.Context = map[string]string{
		"Source": "Table1",
	}
	row := map[string]string{
		"Field1": "field 1 - Test_DBMaster_Table1_Insert - IDU",
		"Field2": "field 2 - Test_DBMaster_Table1_Insert - IDU",
		"Field3": "field 3 - Test_DBMaster_Table1_Insert - IDU",
		"Field4": "field 4 - Test_DBMaster_Table1_Insert - IDU",
	}
	request.Params = map[string]interface{}{
		"Row": row,
	}

	send(conn, request)
	lastInsertedIDDB0 = 1
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "insert")
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "insert")

	if _, err := checkFromTable1(t, dbMaster, lastInsertedIDDB0, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView12, lastInsertedIDDB0, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView23, lastInsertedIDDB0, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView123, lastInsertedIDDB0, row); err != nil {
		t.Fatal(err)
		return
	}

	time.Sleep(600 * time.Millisecond)
	srv.Close()
}

func Test_DBMaster_Table1_Update(t *testing.T) {
	srv, dbMaster, dbView12, dbView23, dbView123 := createSwarm(t)

	srv.RegisterSourceIDU("Table1", Table1Map, dbMaster)
	srv.RegisterTargetIDU("_Table1", Table1Map)

	srv.RegisterSourceIDU("Table2", Table2Map, dbMaster)
	srv.RegisterTargetIDU("_Table2", Table2Map)

	srv.RegisterSourceIDU("Table3", Table3Map, dbMaster)
	srv.RegisterTargetIDU("_Table3", Table3Map)

	srv.RegisterSourceIDU("Table1-Table2", Table1Table2Map, dbView12)
	srv.RegisterSourceIDU("Table2-Table3", Table2Table3Map, dbView23)

	srv.RegisterSourceIDU("Table1-Table2-Table3", Table1Table2Table3Map, dbView123)

	conn, err := net.Dial("tcp", srv.Address)
	if err != nil {
		srv.Close()
		dbMaster.Close()
		dbView12.Close()
		dbView23.Close()
		dbView123.Close()
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-complex-case-update"
	request.Method = "Update"
	request.Context = map[string]string{
		"Source": "Table1",
	}
	row := map[string]string{
		"Field1": "field 1 - Test_DBMaster_Table1_Update - IDU",
		"Field2": "field 2 - Test_DBMaster_Table1_Update - IDU",
		"Field3": "field 3 - Test_DBMaster_Table1_Update - IDU",
		"Field4": "field 4 - Test_DBMaster_Table1_Update - IDU",
	}
	request.Params = map[string]interface{}{
		"Row": row,
		"PK": map[string]int64{
			"ID": lastInsertedIDDB0,
		},
	}

	lastInsertedIDDB0 = 1
	send(conn, request)
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "update")
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "update")

	if _, err := checkFromTable1(t, dbMaster, lastInsertedIDDB0, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView12, lastInsertedIDDB0, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView23, lastInsertedIDDB0, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView123, lastInsertedIDDB0, row); err != nil {
		t.Fatal(err)
		return
	}

	time.Sleep(600 * time.Millisecond)
	srv.Close()
}

func Test_DBMaster_Table1_Delete(t *testing.T) {
	srv, dbMaster, dbView12, dbView23, dbView123 := createSwarm(t)

	srv.RegisterSourceIDU("Table1", Table1Map, dbMaster)
	srv.RegisterTargetIDU("_Table1", Table1Map)

	srv.RegisterSourceIDU("Table2", Table2Map, dbMaster)
	srv.RegisterTargetIDU("_Table2", Table2Map)

	srv.RegisterSourceIDU("Table3", Table3Map, dbMaster)
	srv.RegisterTargetIDU("_Table3", Table3Map)

	srv.RegisterSourceIDU("Table1-Table2", Table1Table2Map, dbView12)
	srv.RegisterSourceIDU("Table2-Table3", Table2Table3Map, dbView23)

	srv.RegisterSourceIDU("Table1-Table2-Table3", Table1Table2Table3Map, dbView123)

	conn, err := net.Dial("tcp", srv.Address)
	if err != nil {
		srv.Close()
		dbMaster.Close()
		dbView12.Close()
		dbView23.Close()
		dbView123.Close()
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-complex-case-delete"
	request.Method = "Delete"
	request.Context = map[string]string{
		"Source": "Table1",
	}
	row := map[string]string{
		"Field1": "field 1 - Test_DBMaster_Table1_Update - IDU",
		"Field2": "field 2 - Test_DBMaster_Table1_Update - IDU",
		"Field3": "field 3 - Test_DBMaster_Table1_Update - IDU",
		"Field4": "field 4 - Test_DBMaster_Table1_Update - IDU",
	}
	request.Params = map[string]interface{}{
		"Row": row,
		"PK": map[string]int64{
			"ID": lastInsertedIDDB0,
		},
	}

	send(conn, request)
	lastInsertedIDDB0 = 1
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "delete")
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "delete")

	if fields, err := checkFromTable1(t, dbMaster, lastInsertedIDDB0, row); err != nil {
		if err == errorField1NotOnlyOne && len(fields) == 0 {
		} else {
			t.Fatal(err)
			return
		}
	}
	if fields, err := checkFromTable1(t, dbView12, lastInsertedIDDB0, row); err != nil {
		if err == errorField1NotOnlyOne && len(fields) == 0 {
		} else {
			t.Fatal(err)
			return
		}
	}
	if fields, err := checkFromTable1(t, dbView23, lastInsertedIDDB0, row); err != nil {
		if err == errorField1NotOnlyOne && len(fields) == 0 {
		} else {
			t.Fatal(err)
			return
		}
	}
	if fields, err := checkFromTable1(t, dbView123, lastInsertedIDDB0, row); err != nil {
		if err == errorField1NotOnlyOne && len(fields) == 0 {
		} else {
			t.Fatal(err)
			return
		}
	}

	time.Sleep(600 * time.Millisecond)
	srv.Close()
}

func Test_DBView12_Table1_Table2_Insert(t *testing.T) {
	srv, dbMaster, dbView12, dbView23, dbView123 := createSwarm(t)

	srv.RegisterSourceIDU("Table1", Table1Map, dbMaster)
	srv.RegisterTargetIDU("_Table1", Table1Map)

	srv.RegisterSourceIDU("Table2", Table2Map, dbMaster)
	srv.RegisterTargetIDU("_Table2", Table2Map)

	srv.RegisterSourceIDU("Table3", Table3Map, dbMaster)
	srv.RegisterTargetIDU("_Table3", Table3Map)

	srv.RegisterSourceIDU("Table1-Table2", Table1Table2Map, dbView12)
	srv.RegisterSourceIDU("Table2-Table3", Table2Table3Map, dbView23)

	srv.RegisterSourceIDU("Table1-Table2-Table3", Table1Table2Table3Map, dbView123)

	conn, err := net.Dial("tcp", srv.Address)
	if err != nil {
		srv.Close()
		dbMaster.Close()
		dbView12.Close()
		dbView23.Close()
		dbView123.Close()
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-table1-table2-case-insert"
	request.Method = "Insert"
	request.Context = map[string]string{
		"Source": "Table1-Table2",
	}
	row12 := map[string]string{
		"Field1": "field 1 - Test_DBView12_Table1_Table2_Insert - IDU",
		"Field2": "field 2 - Test_DBView12_Table1_Table2_Insert - IDU",
		"Field3": "field 3 - Test_DBView12_Table1_Table2_Insert - IDU",
		"Field4": "field 4 - Test_DBView12_Table1_Table2_Insert - IDU",
		"Field5": "field 5 - Test_DBView12_Table1_Table2_Insert - IDU",
		"Field6": "field 6 - Test_DBView12_Table1_Table2_Insert - IDU",
		"Field7": "field 7 - Test_DBView12_Table1_Table2_Insert - IDU",
		"Field8": "field 8 - Test_DBView12_Table1_Table2_Insert - IDU",
	}
	request.Params = map[string]interface{}{
		"Row": row12,
	}

	send(conn, request)
	lastInsertedIDDB0 = 1 // ESTO ES UN ERROR! // ESTO ES UN ERROR! // ESTO ES UN ERROR!
	checkResponseOrNotification(t, conn)
	checkResponseOrNotification(t, conn)
	checkResponseOrNotification(t, conn)

	if _, err := checkFromTable1(t, dbMaster, lastInsertedIDDB0, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView12, lastInsertedIDDB0, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView23, lastInsertedIDDB0, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView123, lastInsertedIDDB0, row12); err != nil {
		t.Fatal(err)
		return
	}

	time.Sleep(600 * time.Millisecond)
	srv.Close()
}

func checkResponseOrNotification(t *testing.T, conn net.Conn) {
	msg := receive(conn)
	if msg.ID != "" {
		t.Log("Revisar Response")
	} else {
		t.Log("Revisar Notification")
	}
	t.Log(msg)
}
