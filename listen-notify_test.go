package dbbus_test

import (
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
	started := make(chan bool)

	db, err := connect()
	if err != nil {
		t.Error(err)
		return
	}
	srv.RegisterDB(connStr, db)

	go func() {
		if err := srv.Start(started); err != nil {
			t.Error(err)
		}
	}()

	if <-started != true {
		t.Error("Unexpected error")
	}
}
