package dbbus_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	dbbus "github.com/m3co/arca-dbbus"
	jsonrpc "github.com/m3co/arca-jsonrpc"
)

func checkType(t *testing.T, typeField, key string, value, actualValue interface{}) {
	row := map[string]interface{}{}
	fieldMap := map[string]string{
		key: typeField,
	}

	expectedColumns := []string{fmt.Sprintf(`"%s"`, key)}
	expectedKeys := []string{key}
	expectedRow := map[string]interface{}{key: actualValue}

	columns, keys, processCells, err := dbbus.PrepareSelectVariables(fieldMap)
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(columns, expectedColumns) {
		t.Fatal(cmp.Diff(columns, expectedColumns))
	}
	if !cmp.Equal(keys, expectedKeys) {
		t.Fatal(cmp.Diff(keys, expectedKeys))
	}
	if err := processCells[0](value, row, key); err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(row, expectedRow) {
		t.Fatal(cmp.Diff(row, expectedRow))
	}
}

func Test_SelectSearch_FieldMap(t *testing.T) {
	checkType(t, "text", "Field1",
		"field 1",
		"field 1")
	checkType(t, "text", "Field1", nil, nil)
	checkType(t, "character varying(255)", "Field1",
		"field 1",
		"field 1")
	checkType(t, "boolean", "Field1",
		true,
		true)
	checkType(t, "boolean", "Field1", nil, nil)
	checkType(t, "integer", "Field1",
		int64(33),
		int64(33))
	checkType(t, "integer", "Field1", nil, nil)
	checkType(t, "double precision", "Field1",
		float64(2.55),
		float64(2.55))
	checkType(t, "double precision", "Field1", nil, nil)
	checkType(t, "numeric(15,2)", "Field1",
		[]byte{49, 53, 54, 46, 50, 50},
		float64(156.22))
	checkType(t, "numeric(15,2)", "Field1", nil, nil)
	checkType(t, "t_enum", "Field1",
		[]byte{49, 53, 54, 46, 50, 50},
		"156.22")
	v, _ := time.Parse(time.RFC3339, "2020-02-01T16:17:18Z")
	checkType(t, "date", "Field1", v, v)
	checkType(t, "timestamp without time zone", "Field1", v, v)
	checkType(t, "timestamp", "Field1", v, v)
}

func Test_SelectSearch_create_server(t *testing.T) {
	connStrSS := ""
	if connStr, db, err := connect("arca-dbbus-db-ss", "test-ss"); err != nil {
		db.Close()
		t.Fatal(err)
		return
	} else {
		dbSS = db
		connStrSS = connStr
	}

	srv := &dbbus.Server{Address: ":22347"}
	started := make(chan bool)

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		dbSS.Close()
		srv.Close()
		t.Fatal("Unexpected error")
		return
	}

	srvSS = srv
	if err := srvSS.RegisterDB(connStrSS, dbSS); err != nil {
		dbSS.Close()
		srv.Close()
		t.Fatal(err)
		return
	}

	srvSS.RegisterSourceIDU("Table1", Table1SSMap, dbSS)
	srvSS.RegisterSourceIDU("Table2", Table2SSMap, dbSS)
}

func Test_SelectSearch_Select_case1(t *testing.T) {
	conn, err := net.Dial("tcp", srvSS.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-sss-select-case-1"
	request.Method = "Select"
	request.Context = map[string]string{
		"Source": "Table2",
	}
	request.Params = map[string]interface{}{}

	send(conn, request)

	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	raw := scanner.Bytes()

	response := map[string]interface{}{}
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
		return
	}
	expected, err := getExpected(t)
	if err != nil {
		t.Fatal(err)
		return
	}
	if !cmp.Equal(response, expected) {
		strToWrite, err := json.MarshalIndent(response, "", "  ")
		if err != nil {
			fmt.Println(err)
			return
		}
		t.Log(string(strToWrite))
		t.Fatal(cmp.Diff(response, expected))
	}
}
