package dbbus

import (
	"database/sql"
	"testing"
)

var connStr = "host=arca-dbbus-db user=test dbname=test password=test port=5432 sslmode=disable"

func connect() (db *sql.DB, err error) {
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return
	}

	err = db.Ping()
	return
}

func Test_check_db(t *testing.T) {
	_, err := connect()

	if err != nil {
		t.Error(err)
	}
}
