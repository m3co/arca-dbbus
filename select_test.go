package dbbus_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"testing"

	"github.com/google/go-cmp/cmp"
	dbbus "github.com/m3co/arca-dbbus"
	jsonrpc "github.com/m3co/arca-jsonrpc"
)

func Test_SelectSearch_FieldMap(t *testing.T) {
	// Estos son los casos a revisar
	// string, 'c3'
	// bool,   true
	// int64,  6
	// slice,  [49 53 54 46 50 50]
	// struct, '2020-02-01 00:00:00 +0000 +0000'
	// struct, '2020-02-01 16:17:18 +0000 +0000'
	// struct, '2020-02-02 15:19:20 +0000 UTC'
	row := map[string]interface{}{}
	fieldMap := map[string]string{
		"Field1": "text",
	}

	expectedColumns := []string{`"Field1"`}
	expectedKeys := []string{`Field1`}
	expectedRow := map[string]interface{}{"Field1": "field1"}

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
	if err := processCells[0]("field1", row, "Field1"); err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(row, expectedRow) {
		t.Fatal(cmp.Diff(row, expectedRow))
	}
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
