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
	request.ID = "jsonrpc-mock-id-case1"
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

	msg1 := receive(conn)
	t.Log(msg1, 1)

	msg2 := receive(conn)
	t.Log(msg2, 2)

	time.Sleep(600 * time.Millisecond)
	srv.Close()
}
