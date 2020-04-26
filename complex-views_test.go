package dbbus_test

import (
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	dbbus "github.com/m3co/arca-dbbus"
	jsonrpc "github.com/m3co/arca-jsonrpc"
)

var (
	srvCmplx                                *dbbus.Server
	dbMaster, dbView12, dbView23, dbView123 *sql.DB
	conn                                    net.Conn
	lastInsertedIDTable1                    int64 = 0
	lastInsertedIDTable2                    int64 = 0
	lastInsertedIDTable3                    int64 = 0
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

	t.Log("dbView12 <")
	showTable1(t, dbView12)

	t.Log("dbView23 <")
	showTable1(t, dbView23)

	t.Log("dbView123 <")
	showTable1(t, dbView123)
}

func showTable2FromAllDBs(t *testing.T) {
	t.Log("dbMaster <")
	showTable2(t, dbMaster)

	t.Log("dbView12 <")
	showTable2(t, dbView12)

	t.Log("dbView23 <")
	showTable2(t, dbView23)

	t.Log("dbView123 <")
	showTable2(t, dbView123)
}

func showTable3FromAllDBs(t *testing.T) {
	t.Log("dbMaster <")
	showTable3(t, dbMaster)

	t.Log("dbView12 <")
	showTable3(t, dbView12)

	t.Log("dbView23 <")
	showTable3(t, dbView23)

	t.Log("dbView123 <")
	showTable3(t, dbView123)
}

func checkResponseOrNotification(t *testing.T, conn net.Conn, method string) {
	msg := receive(conn)
	t.Log(msg)
	if msg.ID != "" {
		checkResponseComplex(t, msg, method)
	} else {
		checkNotificationComplex(t, msg, method)
	}
}

func checkResponseComplex(t *testing.T, response *ResponseOrNotification, method string) {
	if successAndPK, ok := response.Result.(map[string]interface{}); ok {
		if success, ok := successAndPK["Success"].(bool); ok {
			if _, ok := successAndPK["PK"].(map[string]interface{}); ok {
				if success {

				} else {
					t.Fatal("Unexpected to see success = false")
				}
			} else {
				t.Fatal(`successAndPK["PK"].(map[string]interface{}) error`)
			}
		} else {
			t.Fatal(`successAndPK["Success"].(bool) error`)
		}

		if response.Method != method {
			t.Fatal("notification's method expected as insert")
		}
	} else {
		t.Fatal("response.Result.(map[string]interface{}) error")
	}
}

func checkNotificationComplex(t *testing.T, notification *ResponseOrNotification, method string) {
	context, ok := notification.Context.(map[string]interface{})
	if ok {
		iNotification, ok := context["Notification"]
		if ok {
			isNotifcation, ok := iNotification.(bool)
			if ok {
				if isNotifcation {
					if notification.Method != strings.ToLower(method) {
						t.Fatal("notification's method expected as insert")
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
	lastInsertedIDTable1++
	lastInsertedIDDB0 = lastInsertedIDTable1
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "insert")
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "insert")
	conn.Close()
	time.Sleep(600 * time.Millisecond)
	showTable1FromAllDBs(t)

	if _, err := checkFromTable1(t, dbMaster, lastInsertedIDTable1, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView12, lastInsertedIDTable1, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView23, lastInsertedIDTable1, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView123, lastInsertedIDTable1, row); err != nil {
		t.Fatal(err)
		return
	}
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
			"ID": lastInsertedIDTable1,
		},
	}

	send(conn, request)
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "update")
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "update")
	conn.Close()
	time.Sleep(600 * time.Millisecond)
	showTable1FromAllDBs(t)

	if _, err := checkFromTable1(t, dbMaster, lastInsertedIDTable1, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView12, lastInsertedIDTable1, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView23, lastInsertedIDTable1, row); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView123, lastInsertedIDTable1, row); err != nil {
		t.Fatal(err)
		return
	}
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
			"ID": lastInsertedIDTable1,
		},
	}

	send(conn, request)
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "delete")
	testIfResponseOrNotificationOrWhatever(t, conn, dbMaster, row, "delete")
	conn.Close()
	time.Sleep(600 * time.Millisecond)
	showTable1FromAllDBs(t)

	if fields, err := checkFromTable1(t, dbMaster, lastInsertedIDTable1, row); err != nil {
		if err == errorFieldNotOnlyOne && len(fields) == 0 {
		} else {
			t.Fatal(err)
			return
		}
	}
	if fields, err := checkFromTable1(t, dbView12, lastInsertedIDTable1, row); err != nil {
		if err == errorFieldNotOnlyOne && len(fields) == 0 {
		} else {
			t.Fatal(err)
			return
		}
	}
	if fields, err := checkFromTable1(t, dbView23, lastInsertedIDTable1, row); err != nil {
		if err == errorFieldNotOnlyOne && len(fields) == 0 {
		} else {
			t.Fatal(err)
			return
		}
	}
	if fields, err := checkFromTable1(t, dbView123, lastInsertedIDTable1, row); err != nil {
		if err == errorFieldNotOnlyOne && len(fields) == 0 {
		} else {
			t.Fatal(err)
			return
		}
	}
}

func Test_DBView12_Table1_Table2_Insert(t *testing.T) {
	showTable1FromAllDBs(t)
	showTable2FromAllDBs(t)
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
	lastInsertedIDTable1++
	lastInsertedIDTable2++
	checkResponseOrNotification(t, conn, "Insert")
	checkResponseOrNotification(t, conn, "Insert")
	checkResponseOrNotification(t, conn, "Insert")
	conn.Close()
	time.Sleep(600 * time.Millisecond)
	showTable1FromAllDBs(t)
	showTable2FromAllDBs(t)

	if _, err := checkFromTable1(t, dbMaster, lastInsertedIDTable1, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView12, lastInsertedIDTable1, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView23, lastInsertedIDTable1, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView123, lastInsertedIDTable1, row12); err != nil {
		t.Fatal(err)
		return
	}

	if _, err := checkFromTable2(t, dbMaster, lastInsertedIDTable2, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView12, lastInsertedIDTable2, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView23, lastInsertedIDTable2, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView123, lastInsertedIDTable2, row12); err != nil {
		t.Fatal(err)
		return
	}
}

func Test_DBView23_Table2_Table3_Insert(t *testing.T) {
	showTable2FromAllDBs(t)
	showTable3FromAllDBs(t)
	conn, err := net.Dial("tcp", srvCmplx.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-table2-table3-case-insert"
	request.Method = "Insert"
	request.Context = map[string]string{
		"Source": "Table2-Table3",
	}
	row23 := map[string]string{
		"Field5":  "field 5 - Test_DBView23_Table2_Table3_Insert - IDU",
		"Field6":  "field 6 - Test_DBView23_Table2_Table3_Insert - IDU",
		"Field7":  "field 7 - Test_DBView23_Table2_Table3_Insert - IDU",
		"Field8":  "field 8 - Test_DBView23_Table2_Table3_Insert - IDU",
		"Field9":  "field 9 - Test_DBView23_Table2_Table3_Insert - IDU",
		"Field10": "field 10 - Test_DBView23_Table2_Table3_Insert - IDU",
		"Field11": "field 11 - Test_DBView23_Table2_Table3_Insert - IDU",
		"Field12": "field 12 - Test_DBView23_Table2_Table3_Insert - IDU",
	}
	request.Params = map[string]interface{}{
		"Row": row23,
	}

	send(conn, request)
	lastInsertedIDTable2++
	lastInsertedIDTable3++
	checkResponseOrNotification(t, conn, "Insert")
	checkResponseOrNotification(t, conn, "Insert")
	checkResponseOrNotification(t, conn, "Insert")
	conn.Close()
	time.Sleep(600 * time.Millisecond)
	showTable2FromAllDBs(t)
	showTable3FromAllDBs(t)

	if _, err := checkFromTable2(t, dbMaster, lastInsertedIDTable2, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView12, lastInsertedIDTable2, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView23, lastInsertedIDTable2, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView123, lastInsertedIDTable2, row23); err != nil {
		t.Fatal(err)
		return
	}

	if _, err := checkFromTable3(t, dbMaster, lastInsertedIDTable3, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable3(t, dbView12, lastInsertedIDTable3, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable3(t, dbView23, lastInsertedIDTable3, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable3(t, dbView123, lastInsertedIDTable3, row23); err != nil {
		t.Fatal(err)
		return
	}
}

func Test_DBView123_Table1_Table2_Table3_Insert(t *testing.T) {
	showTable1FromAllDBs(t)
	showTable2FromAllDBs(t)
	showTable3FromAllDBs(t)
	conn, err := net.Dial("tcp", srvCmplx.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-table1-table2-table3-case-insert"
	request.Method = "Insert"
	request.Context = map[string]string{
		"Source": "Table1-Table2-Table3",
	}
	row123 := map[string]string{
		"Field1":  "field 1 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
		"Field2":  "field 2 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
		"Field3":  "field 3 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
		"Field4":  "field 4 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
		"Field5":  "field 5 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
		"Field6":  "field 6 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
		"Field7":  "field 7 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
		"Field8":  "field 8 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
		"Field9":  "field 9 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
		"Field10": "field 10 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
		"Field11": "field 11 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
		"Field12": "field 12 - Test_DBView123_Table1_Table2_Table3_Insert - IDU",
	}
	request.Params = map[string]interface{}{
		"Row": row123,
	}

	send(conn, request)
	lastInsertedIDTable1++
	lastInsertedIDTable2++
	lastInsertedIDTable3++
	checkResponseOrNotification(t, conn, "Insert")
	checkResponseOrNotification(t, conn, "Insert")
	checkResponseOrNotification(t, conn, "Insert")
	checkResponseOrNotification(t, conn, "Insert")
	conn.Close()
	time.Sleep(600 * time.Millisecond)
	showTable1FromAllDBs(t)
	showTable2FromAllDBs(t)
	showTable3FromAllDBs(t)

	if _, err := checkFromTable1(t, dbMaster, lastInsertedIDTable1, row123); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView12, lastInsertedIDTable1, row123); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView23, lastInsertedIDTable1, row123); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView123, lastInsertedIDTable1, row123); err != nil {
		t.Fatal(err)
		return
	}

	if _, err := checkFromTable2(t, dbMaster, lastInsertedIDTable2, row123); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView12, lastInsertedIDTable2, row123); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView23, lastInsertedIDTable2, row123); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView123, lastInsertedIDTable2, row123); err != nil {
		t.Fatal(err)
		return
	}

	if _, err := checkFromTable3(t, dbMaster, lastInsertedIDTable3, row123); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable3(t, dbView12, lastInsertedIDTable3, row123); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable3(t, dbView23, lastInsertedIDTable3, row123); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable3(t, dbView123, lastInsertedIDTable3, row123); err != nil {
		t.Fatal(err)
		return
	}
}

func Test_DBView12_Table1_Table2_Update(t *testing.T) {
	showTable1FromAllDBs(t)
	showTable2FromAllDBs(t)
	conn, err := net.Dial("tcp", srvCmplx.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-table1-table2-case-update"
	request.Method = "Update"
	request.Context = map[string]string{
		"Source": "Table1-Table2",
	}
	row12 := map[string]string{
		"Field1": "field 1 - Test_DBView12_Table1_Table2_Update - IDU",
		"Field2": "field 2 - Test_DBView12_Table1_Table2_Update - IDU",
		"Field3": "field 3 - Test_DBView12_Table1_Table2_Update - IDU",
		"Field4": "field 4 - Test_DBView12_Table1_Table2_Update - IDU",
		"Field5": "field 5 - Test_DBView12_Table1_Table2_Update - IDU",
		"Field6": "field 6 - Test_DBView12_Table1_Table2_Update - IDU",
		"Field7": "field 7 - Test_DBView12_Table1_Table2_Update - IDU",
		"Field8": "field 8 - Test_DBView12_Table1_Table2_Update - IDU",
	}
	request.Params = map[string]interface{}{
		"Row": row12,
		"PK": map[string]string{
			"ID1-ID2": fmt.Sprintf("%d-%d", lastInsertedIDTable1, lastInsertedIDTable2),
		},
	}

	send(conn, request)
	checkResponseOrNotification(t, conn, "Update")
	checkResponseOrNotification(t, conn, "Update")
	checkResponseOrNotification(t, conn, "Update")
	conn.Close()
	time.Sleep(600 * time.Millisecond)
	showTable1FromAllDBs(t)
	showTable2FromAllDBs(t)

	if _, err := checkFromTable1(t, dbMaster, lastInsertedIDTable1, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView12, lastInsertedIDTable1, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView23, lastInsertedIDTable1, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable1(t, dbView123, lastInsertedIDTable1, row12); err != nil {
		t.Fatal(err)
		return
	}

	if _, err := checkFromTable2(t, dbMaster, lastInsertedIDTable2, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView12, lastInsertedIDTable2, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView23, lastInsertedIDTable2, row12); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView123, lastInsertedIDTable2, row12); err != nil {
		t.Fatal(err)
		return
	}
}

func Test_DBView23_Table2_Table3_Update(t *testing.T) {
	showTable2FromAllDBs(t)
	showTable3FromAllDBs(t)
	conn, err := net.Dial("tcp", srvCmplx.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-table2-table3-case-Update"
	request.Method = "Update"
	request.Context = map[string]string{
		"Source": "Table2-Table3",
	}
	row23 := map[string]string{
		"Field5":  "field 5 - Test_DBView23_Table2_Table3_Update - IDU",
		"Field6":  "field 6 - Test_DBView23_Table2_Table3_Update - IDU",
		"Field7":  "field 7 - Test_DBView23_Table2_Table3_Update - IDU",
		"Field8":  "field 8 - Test_DBView23_Table2_Table3_Update - IDU",
		"Field9":  "field 9 - Test_DBView23_Table2_Table3_Update - IDU",
		"Field10": "field 10 - Test_DBView23_Table2_Table3_Update - IDU",
		"Field11": "field 11 - Test_DBView23_Table2_Table3_Update - IDU",
		"Field12": "field 12 - Test_DBView23_Table2_Table3_Update - IDU",
	}
	request.Params = map[string]interface{}{
		"Row": row23,
		"PK": map[string]string{
			"ID2-ID3": fmt.Sprintf("%d-%d", lastInsertedIDTable2, lastInsertedIDTable3),
		},
	}

	send(conn, request)
	checkResponseOrNotification(t, conn, "Update")
	checkResponseOrNotification(t, conn, "Update")
	checkResponseOrNotification(t, conn, "Update")
	conn.Close()
	time.Sleep(600 * time.Millisecond)
	showTable2FromAllDBs(t)
	showTable3FromAllDBs(t)

	if _, err := checkFromTable2(t, dbMaster, lastInsertedIDTable2, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView12, lastInsertedIDTable2, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView23, lastInsertedIDTable2, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable2(t, dbView123, lastInsertedIDTable2, row23); err != nil {
		t.Fatal(err)
		return
	}

	if _, err := checkFromTable3(t, dbMaster, lastInsertedIDTable3, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable3(t, dbView12, lastInsertedIDTable3, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable3(t, dbView23, lastInsertedIDTable3, row23); err != nil {
		t.Fatal(err)
		return
	}
	if _, err := checkFromTable3(t, dbView123, lastInsertedIDTable3, row23); err != nil {
		t.Fatal(err)
		return
	}
}
