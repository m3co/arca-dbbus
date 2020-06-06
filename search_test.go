package dbbus_test

import (
	"testing"

	dbbus "github.com/m3co/arca-dbbus"
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
