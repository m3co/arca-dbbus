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

func Test_Search_create_server(t *testing.T) {
	connStrSearch := ""
	if connStr, db, err := connect("arca-dbbus-db-search", "test-search"); err != nil {
		db.Close()
		t.Fatal(err)
		return
	} else {
		dbSearch = db
		connStrSearch = connStr
	}

	srv := &dbbus.Server{Address: ":22348"}
	started := make(chan bool)

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		dbSearch.Close()
		srv.Close()
		t.Fatal("Unexpected error")
		return
	}

	srvSearch = srv
	if err := srvSearch.RegisterDB(connStrSearch, dbSearch); err != nil {
		dbSearch.Close()
		srv.Close()
		t.Fatal(err)
		return
	}

	srvSearch.RegisterSourceIDU("Table2", Table2SSMap(), dbSearch)
}

func Test_Search_case1_noparams(t *testing.T) {
	conn, err := net.Dial("tcp", srvSearch.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-searchs-select-case-1"
	request.Method = "Search"
	request.Context = map[string]string{
		"Source": "Table2",
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

func Test_Search_case2_incorrect_params(t *testing.T) {
	conn, err := net.Dial("tcp", srvSearch.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-searchs-select-case-2"
	request.Method = "Search"
	request.Context = map[string]string{
		"Source": "Table2",
	}
	request.Params = 22

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

func Test_Search_case3_no_search_param(t *testing.T) {
	conn, err := net.Dial("tcp", srvSearch.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-searchs-select-case-3"
	request.Method = "Search"
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

func Test_Search_case4_search_param_empty(t *testing.T) {
	conn, err := net.Dial("tcp", srvSearch.Address)
	if err != nil {
		t.Fatal(err)
		return
	}

	request := &jsonrpc.Request{}
	request.ID = "jsonrpc-mock-id-searchs-select-case-4"
	request.Method = "Search"
	request.Context = map[string]string{
		"Source": "Table2",
	}
	request.Params = map[string]string{
		"Search": "",
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
