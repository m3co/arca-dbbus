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

func Test_SelectSearch_Select_case2(t *testing.T) {
	conn, err := net.Dial("tcp", srvSS.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-sss-select-case-2"
	request.Method = "Select"
	request.Context = map[string]string{
		"Source": "Table1",
	}
	request.Params = map[string]interface{}{
		"PK": map[string]interface{}{
			"Field2": "ab1",
		},
	}

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

func Test_SelectSearch_Select_case3(t *testing.T) {
	conn, err := net.Dial("tcp", srvSS.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-sss-select-case-3"
	request.Method = "Select"
	request.Context = map[string]string{
		"Source": "Table1",
	}
	request.Params = map[string]interface{}{
		"PK": map[string]interface{}{
			"Field2": "bc%",
		},
	}

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

func Test_SelectSearch_Select_case4(t *testing.T) {
	conn, err := net.Dial("tcp", srvSS.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-sss-select-case-4"
	request.Method = "Select"
	request.Context = map[string]string{
		"Source": "Table1",
	}
	request.Params = map[string]interface{}{
		"PK": map[string]interface{}{
			"Field2": "%c2",
		},
	}

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

func Test_SelectSearch_Select_case5(t *testing.T) {
	conn, err := net.Dial("tcp", srvSS.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-sss-select-case-5"
	request.Method = "Search"
	request.Context = map[string]string{
		"Source": "Table1",
	}
	request.Params = map[string]interface{}{
		"PK": map[string]interface{}{
			"Field3": "search%",
		},
	}

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
