package dbbus_test

import (
	"testing"

	dbbus "github.com/m3co/arca-dbbus"
)

func Test_Search_create_server2(t *testing.T) {
	connStrSearch := ""
	if connStr, db, err := connect("arca-dbbus-db-search", "test-search"); err != nil {
		db.Close()
		t.Fatal(err)
		return
	} else {
		dbSearch2 = db
		connStrSearch = connStr
	}

	srv := &dbbus.Server{Address: ":22349"}
	started := make(chan bool)

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		dbSearch2.Close()
		srv.Close()
		t.Fatal("Unexpected error")
		return
	}

	srvSearch2 = srv
	if err := srvSearch2.RegisterDB(connStrSearch, dbSearch2); err != nil {
		dbSearch2.Close()
		srv.Close()
		t.Fatal(err)
		return
	}

	labeler := func(row map[string]interface{}) (string, error) {
		return row["Field2"].(string), nil
	}
	srvSearch2.RegisterSourceIDU("Table2", Table2SSMap(), dbSearch2)
	srvSearch2.RegisterSourceSearch("Table2", Table2SSMap(), dbSearch2, labeler)
}
