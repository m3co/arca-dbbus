package dbbus_test

import (
	"database/sql"
	"testing"

	dbbus "github.com/m3co/arca-dbbus"
)

// Fields is the struct for the Table
type Fields struct {
	ID                             int64
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

func selectFieldsFromTable(db *sql.DB) (fields []Fields, err error) {
	var rows *sql.Rows
	fields = []Fields{}
	rows, err = db.Query(`select "ID", "Field1", "Field2", "Field3", "Field4" from "Table" order by "ID"`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var ID int64
		var Field1, Field2, Field3, Field4 string
		if err = rows.Scan(&ID, &Field1, &Field2, &Field3, &Field4); err != nil {
			return
		}
		fields = append(fields, Fields{
			ID,
			Field1,
			Field2,
			Field3,
			Field4,
		})
	}
	err = rows.Err()
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
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) > 0 {
		t.Fatal("Table must be empty")
	}
}

func Test_prepareAndExecute_do_insert__OK(t *testing.T) {
	db, err := connect()
	if err != nil {
		t.Fatal(err)
	}
	result, err := dbbus.PrepareAndExecute(db,
		`insert into "Table"("Field1", "Field2", "Field3", "Field4") values ($1, $2, $3, $4);`,
		"field 1", "field 2", "field 3", "field 4")

	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("unexpected result")
	}
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if !(field.ID == 1 &&
			field.Field1 == "field 1" &&
			field.Field2 == "field 2" &&
			field.Field3 == "field 3" &&
			field.Field4 == "field 4") {
			t.Fatal("Unexpected field")
		}
	}
}
