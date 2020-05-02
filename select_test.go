package dbbus_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"testing"

	dbbus "github.com/m3co/arca-dbbus"
	jsonrpc "github.com/m3co/arca-jsonrpc"
)

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
	}
	expected := map[string]interface{}{
		"Context": map[string]interface{}{"Source": "Table2"},
		"Error":   nil,
		"ID":      "jsonrpc-mock-id-sss-select-case-1",
		"Method":  "Select",
		"Result": []map[string]interface{}{
			map[string]interface{}{
				"Field1": nil,
				"Field2": nil,
				"Field3": nil,
				"Field4": nil,
				"Field5": nil,
				"Field6": nil,
				"Field7": nil,
				"Field8": nil,
				"ID":     1,
			},
			map[string]interface{}{
				"Field1": "Character Varying 255",
				"Field2": nil,
				"Field3": nil,
				"Field4": nil,
				"Field5": nil,
				"Field6": nil,
				"Field7": nil,
				"Field8": nil,
				"ID":     2,
			},
			map[string]interface{}{
				"Field1": nil,
				"Field2": "Text",
				"Field3": nil,
				"Field4": nil,
				"Field5": nil,
				"Field6": nil,
				"Field7": nil,
				"Field8": nil,
				"ID":     3,
			},
			map[string]interface{}{
				"Field1": nil,
				"Field2": nil,
				"Field3": 156.22,
				"Field4": nil,
				"Field5": nil,
				"Field6": nil,
				"Field7": nil,
				"Field8": nil,
				"ID":     4,
			},
			map[string]interface{}{
				"Field1": nil,
				"Field2": nil,
				"Field3": nil,
				"Field4": true,
				"Field5": nil,
				"Field6": nil,
				"Field7": nil,
				"Field8": nil,
				"ID":     5,
			},
			map[string]interface{}{
				"Field1": nil,
				"Field2": nil,
				"Field3": nil,
				"Field4": false,
				"Field5": nil,
				"Field6": nil,
				"Field7": nil,
				"Field8": nil,
				"ID":     6,
			},
			map[string]interface{}{
				"Field1": nil,
				"Field2": nil,
				"Field3": nil,
				"Field4": nil,
				"Field5": "2020-02-01T00:00:00Z",
				"Field6": nil,
				"Field7": nil,
				"Field8": nil,
				"ID":     7,
			},
			map[string]interface{}{
				"Field1": nil,
				"Field2": nil,
				"Field3": nil,
				"Field4": nil,
				"Field5": nil,
				"Field6": "2020-02-01T16:17:18Z",
				"Field7": nil,
				"Field8": nil,
				"ID":     8,
			},
			map[string]interface{}{
				"Field1": nil,
				"Field2": nil,
				"Field3": nil,
				"Field4": nil,
				"Field5": nil,
				"Field6": nil,
				"Field7": "2020-02-02T15:19:20Z",
				"Field8": nil,
				"ID":     9,
			},
			map[string]interface{}{
				"Field1": nil,
				"Field2": nil,
				"Field3": nil,
				"Field4": nil,
				"Field5": nil,
				"Field6": nil,
				"Field7": nil,
				"Field8": "T-ENUM",
				"ID":     10,
			},
		},
	}

	actualStr := fmt.Sprintf("%v", response)
	expectStr := fmt.Sprintf("%v", expected)

	if expectStr != actualStr {
		t.Fatal("expected doesn't match actual")
	}
}
