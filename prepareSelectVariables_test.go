package dbbus

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func checkType(t *testing.T, typeField, key string, value, actualValue interface{}) {
	row := map[string]interface{}{}
	fieldMap := map[string]string{
		key: typeField,
	}

	expectedColumns := []string{fmt.Sprintf(`"%s"`, key)}
	expectedKeys := []string{key}
	expectedRow := map[string]interface{}{key: actualValue}

	columns, keys, processCells, err := prepareSelectVariables(fieldMap)
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(columns, expectedColumns) {
		t.Fatal(cmp.Diff(columns, expectedColumns))
	}
	if !cmp.Equal(keys, expectedKeys) {
		t.Fatal(cmp.Diff(keys, expectedKeys))
	}
	if err := processCells[0](value, row, key); err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(row, expectedRow) {
		t.Fatal(cmp.Diff(row, expectedRow))
	}
}

func Test_SelectSearch_FieldMap(t *testing.T) {
	checkType(t, "text", "Field1",
		"field 1",
		"field 1")
	checkType(t, "text", "Field1", nil, nil)
	checkType(t, "character varying(255)", "Field1",
		"field 1",
		"field 1")
	checkType(t, "boolean", "Field1",
		true,
		true)
	checkType(t, "boolean", "Field1", nil, nil)
	checkType(t, "integer", "Field1",
		int64(33),
		int64(33))
	checkType(t, "integer", "Field1", nil, nil)
	checkType(t, "double precision", "Field1",
		float64(2.55),
		float64(2.55))
	checkType(t, "double precision", "Field1", nil, nil)
	checkType(t, "numeric(15,2)", "Field1",
		[]byte{49, 53, 54, 46, 50, 50},
		float64(156.22))
	checkType(t, "numeric(15,2)", "Field1", nil, nil)
	checkType(t, "t_enum", "Field1",
		[]byte{49, 53, 54, 46, 50, 50},
		"156.22")
	v, _ := time.Parse(time.RFC3339, "2020-02-01T16:17:18Z")
	checkType(t, "date", "Field1", v, v)
	checkType(t, "timestamp without time zone", "Field1", v, v)
	checkType(t, "timestamp", "Field1", v, v)
}
