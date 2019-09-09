package dbbus

import (
	"database/sql"
	"testing"
)

// Fields is the struct for the Table
type Fields struct {
	Field1, Field2, Field3, Field4 string
}

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
		t.Fatal(err)
	}
}

func Test_select_Table_empty__OK(t *testing.T) {
	db, err := connect()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`select "Field1", "Field2", "Field3", "Field4" from "Table"`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var field Fields
		if err := rows.Scan(&field); err != nil {
			t.Fatal(err)
		}
		t.Fatal("Table must be empty")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
