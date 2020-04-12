package dbbus_test

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

var (
	connStr  = ""
	fieldMap = map[string]string{
		"ID":     "integer",
		"Field1": "character varying(255)",
		"Field2": "character varying(255)",
		"Field3": "character varying(255)",
		"Field4": "character varying(255)",
	}
	PK                   = []string{"ID"}
	lastInsertedID int64 = 0
)

func fieldmap() (map[string]string, []string) {
	return fieldMap, PK
}

func defineVariables() {
	dbhost := "arca-dbbus-db"
	err := godotenv.Load()
	if err == nil {
		dbhost = os.Getenv("DB_HOST")
	}
	connStr = fmt.Sprintf("host=%s user=test dbname=test password=test port=5432 sslmode=disable", dbhost)
	fmt.Println(connStr)
}

func init() {
	if connStr == "" {
		defineVariables()
	}
}

// Fields is the struct for the Table
type Fields struct {
	ID             int64
	Field1, Field4 *string
	Field2, Field3 string
}

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
	rows, err = db.Query(`select "ID", "Field1", "Field2", "Field3", "Field4" from "_Table" order by "ID"`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var ID int64
		var Field1, Field4 *string
		var Field2, Field3 string
		if err = rows.Scan(&ID, &Field1, &Field2, &Field3, &Field4); err != nil {
			return
		}
		fields = append(fields, Fields{
			ID:     ID,
			Field1: Field1,
			Field2: Field2,
			Field3: Field3,
			Field4: Field4,
		})
	}
	err = rows.Err()
	return
}
