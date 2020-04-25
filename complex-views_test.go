package dbbus_test

import (
	"database/sql"
	"net"
	"testing"
	"time"

	dbbus "github.com/m3co/arca-dbbus"
	jsonrpc "github.com/m3co/arca-jsonrpc"
)

var (
	srvCmplx                                *dbbus.Server
	dbMaster, dbView12, dbView23, dbView123 *sql.DB
	conn                                    net.Conn
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

func showTable1FromAllDBs(t *testing.T) {
	t.Log("dbMaster <")
	showTable1(t, dbMaster)
	t.Log("dbMaster >")

	t.Log("dbView12 <")
	showTable1(t, dbView12)
	t.Log("dbView12 >")

	t.Log("dbView23 <")
	showTable1(t, dbView23)
	t.Log("dbView23 >")

	t.Log("dbView123 <")
	showTable1(t, dbView123)
	t.Log("dbView123 >")
}

func Test_check_allDBs(t *testing.T) {
	srvCmplx, dbMaster, dbView12, dbView23, dbView123 = createSwarm(t)

	srvCmplx.RegisterSourceIDU("Table1", Table1Map, dbMaster)
	srvCmplx.RegisterTargetIDU("_Table1", Table1Map)

	srvCmplx.RegisterSourceIDU("Table2", Table2Map, dbMaster)
	srvCmplx.RegisterTargetIDU("_Table2", Table2Map)

	srvCmplx.RegisterSourceIDU("Table3", Table3Map, dbMaster)
	srvCmplx.RegisterTargetIDU("_Table3", Table3Map)

	srvCmplx.RegisterSourceIDU("Table1-Table2", Table1Table2Map, dbView12)
	srvCmplx.RegisterSourceIDU("Table2-Table3", Table2Table3Map, dbView23)
	srvCmplx.RegisterSourceIDU("Table1-Table2-Table3", Table1Table2Table3Map, dbView123)

	showTable1FromAllDBs(t)
}

func Test_DBMaster_Table1_Insert(t *testing.T) {
	conn, err := net.Dial("tcp", srvCmplx.Address)
	if err != nil {
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
	conn.Close()

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

	showTable1FromAllDBs(t)
	time.Sleep(600 * time.Millisecond)
}

func Test_DBMaster_Table1_Update(t *testing.T) {
	conn, err := net.Dial("tcp", srvCmplx.Address)
	if err != nil {
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

	send(conn, request)
	lastInsertedIDDB0 = 1
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "update")
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "update")
	conn.Close()

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

	showTable1FromAllDBs(t)
	time.Sleep(600 * time.Millisecond)
}

func Test_DBMaster_Table1_Delete(t *testing.T) {
	conn, err := net.Dial("tcp", srvCmplx.Address)
	if err != nil {
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
	conn.Close()

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

	showTable1FromAllDBs(t)
	time.Sleep(600 * time.Millisecond)
}

func Test_DBView12_Table1_Table2_Insert(t *testing.T) {
	showTable1FromAllDBs(t)
	conn, err := net.Dial("tcp", srvCmplx.Address)
	if err != nil {
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
	conn.Close()

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

	showTable1FromAllDBs(t)
	time.Sleep(600 * time.Millisecond)
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
