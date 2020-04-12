package dbbus_test

import (
	"net"
	"testing"

	dbbus "github.com/m3co/arca-dbbus"
)

func init() {
	if connStr == "" {
		defineVariables()
	}
}

func Test_connect_RegisterDB(t *testing.T) {
	srv := dbbus.Server{}
	defer srv.Close()
	started := make(chan bool)

	db, err := connect()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer db.Close()
	srv.RegisterDB(connStr, db)

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		t.Fatal("Unexpected error")
	}
}

func Test_call_RegisterIDU(t *testing.T) {
	srv := dbbus.Server{}
	defer srv.Close()
	started := make(chan bool)

	db, err := connect()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer db.Close()
	srv.RegisterDB(connStr, db)

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		t.Fatal("Unexpected error")
	}

	srv.RegisterDB(connStr, db)
	srv.RegisterSourceIDU("Table", fieldmap, db)
	srv.RegisterTargetIDU("_Table", fieldmap)
}

func Test_call_RegisterIDU_connect(t *testing.T) {
	srv := dbbus.Server{}
	defer srv.Close()
	started := make(chan bool)

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		t.Fatal("Unexpected error")
	}

	conn, err := net.Dial("tcp", srv.Address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
}
