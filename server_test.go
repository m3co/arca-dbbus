package dbbus_test

import (
	"testing"

	dbbus "github.com/m3co/arca-dbbus"
)

/* Casos
Field1	-			-
Field2	not null	-
Field3	-			default
Field4	not null	default
*/

func Test_check_db(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}

func Test_prepareAndExecute_do_insert__take1_OK(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	result, err := dbbus.PrepareAndExecute(db, []string{"ID", "Field2"},
		`insert into "_Table1"("Field1", "Field2", "Field3", "Field4")
		 values ($1, $2, $3, $4) returning "ID", "Field2";`,
		"take 1 - field 1", "take 1 - field 2", "take 1 - field 3", "take 1 - field 4")
	if err != nil {
		t.Fatal(err)
	}
	ID, ok := result.PK["ID"]
	lastInsertedID++
	if !(ok && ID.(int64) == lastInsertedID) {
		t.Fatal("unexpected ID at result")
	}
	Field2, ok := result.PK["Field2"]
	if !(ok && Field2.(string) == "take 1 - field 2") {
		t.Fatal("unexpected Field2 at result")
	}
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(*field.Field1 == "take 1 - field 1" &&
			field.Field2 == "take 1 - field 2" &&
			field.Field3 == "take 1 - field 3" &&
			*field.Field4 == "take 1 - field 4") {
			t.Fatal("Unexpected row at take 1")
		}
	}
}

func Test_prepareAndExecute_do_insert__take2_OK(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = dbbus.PrepareAndExecute(db, nil,
		`insert into "_Table1"("Field2", "Field3", "Field4")
		 values ($1, $2, $3);`,
		"take 2 - field 2", "take 2 - field 3", "take 2 - field 4")

	if err != nil {
		t.Fatal(err)
	}
	lastInsertedID++
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	atLeastOneRun := false
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(field.Field1 == nil &&
			field.Field2 == "take 2 - field 2" &&
			field.Field3 == "take 2 - field 3" &&
			*field.Field4 == "take 2 - field 4") {
			t.Fatal("Unexpected row at take 2")
		}
		atLeastOneRun = true
	}
	if atLeastOneRun == false {
		t.Fatal("Nothing was tested at Test_prepareAndExecute_do_insert__take2_OK")
	}
}

func Test_prepareAndExecute_do_insert__take3_OK(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = dbbus.PrepareAndExecute(db, nil,
		`insert into "_Table1"("Field1", "Field2", "Field3", "Field4")
		 values ($1, $2, $3, $4);`,
		nil, "take 3 - field 2", "take 3 - field 3", "take 3 - field 4")

	if err != nil {
		t.Fatal(err)
	}
	lastInsertedID++
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	atLeastOneRun := false
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(field.Field1 == nil &&
			field.Field2 == "take 3 - field 2" &&
			field.Field3 == "take 3 - field 3" &&
			*field.Field4 == "take 3 - field 4") {
			t.Fatal("Unexpected row at take 3")
		}
		atLeastOneRun = true
	}
	if atLeastOneRun == false {
		t.Fatal("Nothing was tested at Test_prepareAndExecute_do_insert__take3_OK")
	}
}

func Test_prepareAndExecute_do_insert__take4_OK(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = dbbus.PrepareAndExecute(db, nil,
		`insert into "_Table1"("Field1", "Field2", "Field3", "Field4")
		 values ($1, $2, $3, $4);`,
		nil, "take 4 - field 2", "take 4 - field 3", "take 4 - field 4")

	if err != nil {
		t.Fatal(err)
	}
	lastInsertedID++
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	atLeastOneRun := false
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(field.Field1 == nil &&
			field.Field2 == "take 4 - field 2" &&
			field.Field3 == "take 4 - field 3" &&
			*field.Field4 == "take 4 - field 4") {
			t.Fatal("Unexpected row at take 4")
		}
		atLeastOneRun = true
	}
	if atLeastOneRun == false {
		t.Fatal("Nothing was tested at Test_prepareAndExecute_do_insert__take3_OK")
	}
}

func Test_insert__undefined_row_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{}
	_, err = dbbus.Insert(db, params, fieldMap, nil, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorUndefinedRow {
		t.Fatal(err)
	}
}

func Test_insert__zeroparams_row_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{
		"Row": map[string]interface{}{},
	}
	_, err = dbbus.Insert(db, params, fieldMap, nil, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorZeroParamsInRow {
		t.Fatal(err)
	}
}

func Test_insert__malformed_row_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{
		"Row": 666,
	}
	_, err = dbbus.Insert(db, params, fieldMap, nil, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorMalformedRow {
		t.Fatal(err)
	}
}

func Test_insert__take1_OK(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := map[string]interface{}{
		"Field1": "insert - take 1 - field 1",
		"Field2": "insert - take 1 - field 2",
		"Field3": "insert - take 1 - field 3",
		"Field4": "insert - take 1 - field 4",
	}
	params := map[string]interface{}{
		"Row": row,
	}
	pk := []string{"ID", "Field2"}
	result, err := dbbus.Insert(db, params, fieldMap, pk, "_Table1")
	lastInsertedID++
	if err != nil {
		t.Fatal(err)
	}
	ID, ok := result.PK["ID"]
	if !ok {
		t.Fatal("Expecting ID in result")
	}
	if id, ok := ID.(int64); ok {
		if id != lastInsertedID {
			t.Fatal("Unexpected ID")
		}
	} else {
		t.Fatal("Cannot cast ID")
	}
	Field2, ok := row["Field2"]
	if !ok {
		t.Fatal("Expecting Field2 in result")
	}
	if field2, ok := Field2.(string); ok {
		if field2 != "insert - take 1 - field 2" {
			t.Fatal("Unexpected Field2")
		}
	} else {
		t.Fatal("Cannot cast Field2")
	}
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(*field.Field1 == "insert - take 1 - field 1" &&
			field.Field2 == "insert - take 1 - field 2" &&
			field.Field3 == "insert - take 1 - field 3" &&
			*field.Field4 == "insert - take 1 - field 4") {
			t.Fatal("Unexpected row at insert - take 1")
		}
	}
}

func Test_update__undefined_row_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{}
	_, err = dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorUndefinedRow {
		t.Fatal(err)
	}
}

func Test_update__undefined_pk_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{
		"Row": map[string]interface{}{
			"Field1": "whatever",
		},
	}
	_, err = dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorUndefinedPK {
		t.Fatal(err)
	}
}

func Test_update__zeroparams_row_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{
		"Row": map[string]interface{}{},
		"PK":  map[string]interface{}{},
	}
	_, err = dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorZeroParamsInRow {
		t.Fatal(err)
	}
}

func Test_update__zeroparams_pk_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{
		"Row": map[string]interface{}{
			"Field1": "whatever",
		},
		"PK": map[string]interface{}{},
	}
	_, err = dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorZeroParamsInPK {
		t.Fatal(err)
	}
}

func Test_update__malformed_row_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{
		"Row": 666,
		"PK":  map[string]interface{}{},
	}
	_, err = dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorMalformedRow {
		t.Fatal(err)
	}
}

func Test_update__malformed_pk_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{
		"Row": map[string]interface{}{
			"Field1": "whatever",
		},
		"PK": 666,
	}
	_, err = dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorMalformedPK {
		t.Fatal(err)
	}
}

func Test_update__emptycondition_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := map[string]interface{}{
		"Field1": "Whatever",
	}
	pk := map[string]interface{}{
		"Field1": "Whatever",
	}
	params := map[string]interface{}{
		"Row": row,
		"PK":  pk,
	}
	_, err = dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorEmptyCondition {
		t.Fatal(err)
	}
}

func Test_update__take1_OK(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := map[string]interface{}{
		"Field1": "update - take 1 - field 1",
		"Field2": "update - take 1 - field 2",
		"Field3": "update - take 1 - field 3",
		"Field4": "update - take 1 - field 4",
	}
	pk := map[string]interface{}{
		"ID": lastInsertedID,
	}
	params := map[string]interface{}{
		"Row": row,
		"PK":  pk,
	}
	result, err := dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err != nil {
		t.Fatal(err)
	}
	ID, ok := result.PK["ID"]
	if !ok {
		t.Fatal("Expecting ID in result")
	}
	if id, ok := ID.(int64); ok {
		if id != lastInsertedID {
			t.Fatal("Unexpected ID")
		}
	} else {
		t.Fatal("Cannot cast ID")
	}
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(*field.Field1 == "update - take 1 - field 1" &&
			field.Field2 == "update - take 1 - field 2" &&
			field.Field3 == "update - take 1 - field 3" &&
			*field.Field4 == "update - take 1 - field 4") {
			t.Fatal("Unexpected row at update - take 1")
		}
	}
}

func Test_update__take2_OK(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := map[string]interface{}{
		"Field2": "update - take 2 - field 2",
		"Field3": "update - take 2 - field 3",
		"Field4": "update - take 2 - field 4",
	}
	pk := map[string]interface{}{
		"ID": lastInsertedID,
	}
	params := map[string]interface{}{
		"Row": row,
		"PK":  pk,
	}
	_, err = dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(*field.Field1 == "update - take 1 - field 1" &&
			field.Field2 == "update - take 2 - field 2" &&
			field.Field3 == "update - take 2 - field 3" &&
			*field.Field4 == "update - take 2 - field 4") {
			t.Fatal("Unexpected row at update - take 2")
		}
	}
}

func Test_update__take3_OK(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := map[string]interface{}{
		"Field3": "update - take 3 - field 3",
		"Field4": "update - take 3 - field 4",
	}
	pk := map[string]interface{}{
		"ID": lastInsertedID,
	}
	params := map[string]interface{}{
		"Row": row,
		"PK":  pk,
	}
	_, err = dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(*field.Field1 == "update - take 1 - field 1" &&
			field.Field2 == "update - take 2 - field 2" &&
			field.Field3 == "update - take 3 - field 3" &&
			*field.Field4 == "update - take 3 - field 4") {
			t.Fatal("Unexpected row at update - take 3")
		}
	}
}

func Test_update__take4_OK(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := map[string]interface{}{
		"Field4": "update - take 4 - field 4",
	}
	pk := map[string]interface{}{
		"ID": lastInsertedID,
	}
	params := map[string]interface{}{
		"Row": row,
		"PK":  pk,
	}
	_, err = dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(*field.Field1 == "update - take 1 - field 1" &&
			field.Field2 == "update - take 2 - field 2" &&
			field.Field3 == "update - take 3 - field 3" &&
			*field.Field4 == "update - take 4 - field 4") {
			t.Fatal("Unexpected row at update - take 4")
		}
	}
}

func Test_update__take5_OK(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := map[string]interface{}{
		"Field1": nil,
	}
	pk := map[string]interface{}{
		"ID": lastInsertedID,
	}
	params := map[string]interface{}{
		"Row": row,
		"PK":  pk,
	}
	_, err = dbbus.Update(db, params, fieldMap, PK, "_Table1")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if field.ID != lastInsertedID {
			continue
		}
		if !(field.Field1 == nil &&
			field.Field2 == "update - take 2 - field 2" &&
			field.Field3 == "update - take 3 - field 3" &&
			*field.Field4 == "update - take 4 - field 4") {
			t.Fatal("Unexpected row at update - take 4")
		}
	}
}

func Test_delete__undefined_pk_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{}
	_, err = dbbus.Delete(db, params, fieldMap, nil, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorUndefinedPK {
		t.Fatal(err)
	}
}

func Test_delete__zeroparams_pk_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{
		"PK": map[string]interface{}{},
	}
	_, err = dbbus.Delete(db, params, fieldMap, nil, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorZeroParamsInPK {
		t.Fatal(err)
	}
}

func Test_delete__malformed_pk_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	params := map[string]interface{}{
		"PK": 666,
	}
	_, err = dbbus.Delete(db, params, fieldMap, nil, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorMalformedPK {
		t.Fatal(err)
	}
}

func Test_delete__take1_OK(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pk := map[string]interface{}{
		"ID": lastInsertedID,
	}
	params := map[string]interface{}{
		"PK": pk,
	}
	result, err := dbbus.Delete(db, params, fieldMap, PK, "_Table1")
	if err != nil {
		t.Fatal(err)
	}
	ID, ok := result.PK["ID"]
	if !ok {
		t.Fatal("Expecting ID in result")
	}
	if id, ok := ID.(int64); ok {
		if id != lastInsertedID {
			t.Fatal("Unexpected ID")
		}
	} else {
		t.Fatal("Cannot cast ID")
	}
	fields, err := selectFieldsFromTable(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if field.ID == lastInsertedID {
			t.Fatal("Unexpected row at delete - take 1")
		}
	}
}

/* This is a case temporary I need to skip
func Test_delete__emptycondition_ERROR(t *testing.T) {
	_, db, err := connect("arca-dbbus-db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pk := map[string]interface{}{
		"Whatever": "Whatever",
	}
	params := map[string]interface{}{
		"PK": pk,
	}
	_, err = dbbus.Delete(db, params, fieldMap, nil, "_Table1")
	if err == nil {
		t.Fatal("error expected")
	}
	if err != dbbus.ErrorEmptyCondition {
		t.Fatal(err)
	}
}
*/
