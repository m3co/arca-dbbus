package dbbus_test

import (
	"testing"

	dbbus "github.com/m3co/arca-dbbus"
)

// Case 1: PK has no params
func Test_wherePK_case1(t *testing.T) {
	PK := map[string]interface{}{}
	fieldMap := map[string]string{}
	keys := []string{}
	values := &([]interface{}{})

	if _, err := dbbus.WherePK(PK, fieldMap, keys, values, 0); err != nil {
		if err != dbbus.ErrorZeroParamsInPK {
			t.Fatal(err)
		}
	}
}

// Case 2: fieldMap is empty
func Test_wherePK_case2(t *testing.T) {
	PK := map[string]interface{}{"ID": "integer"}
	fieldMap := map[string]string{}
	keys := []string{}
	values := &([]interface{}{})

	if _, err := dbbus.WherePK(PK, fieldMap, keys, values, 0); err != nil {
		if err != dbbus.ErrorZeroParamsInFieldMap {
			t.Fatal(err)
		}
	}
}

// Case 3: keys is empty
func Test_wherePK_case3(t *testing.T) {
	PK := map[string]interface{}{"ID": 2}
	fieldMap := map[string]string{"ID": "integer"}
	keys := []string{}
	values := &([]interface{}{})

	if _, err := dbbus.WherePK(PK, fieldMap, keys, values, 0); err != nil {
		if err != dbbus.ErrorZeroParamsInKeys {
			t.Fatal(err)
		}
	}
}

// Case 4: values is not defined
func Test_wherePK_case4(t *testing.T) {
	PK := map[string]interface{}{"ID": 2}
	fieldMap := map[string]string{"ID": "integer"}
	keys := []string{"ID"}
	var values *[]interface{}

	if _, err := dbbus.WherePK(PK, fieldMap, keys, values, 0); err != nil {
		if err != dbbus.ErrorUndefinedValuesArray {
			t.Fatal(err)
		}
	}
}

// Case 5: index is a negative number
func Test_wherePK_case5(t *testing.T) {
	PK := map[string]interface{}{"ID": 2}
	fieldMap := map[string]string{"ID": "integer"}
	keys := []string{"ID"}
	values := &[]interface{}{}

	if _, err := dbbus.WherePK(PK, fieldMap, keys, values, -1); err != nil {
		if err != dbbus.ErrorIndexNegative {
			t.Fatal(err)
		}
	}
}

// Case 5: simple case
func Test_wherePK_result_case5(t *testing.T) {
	PK := map[string]interface{}{"ID": 2}
	fieldMap := map[string]string{"ID": "integer"}
	keys := []string{"ID"}
	values := &[]interface{}{}
	expected := `"ID"=$1::integer`

	if actual, err := dbbus.WherePK(PK, fieldMap, keys, values, 0); err != nil {
		t.Fatal(err)
	} else {
		if actual != expected {
			t.Fatalf("\nExpect %s\nActual %s", expected, actual)
		}
	}
}

// Case 6: two PK entries
func Test_wherePK_result_case6(t *testing.T) {
	PK := map[string]interface{}{"ID": 2, "Key": "key"}
	fieldMap := map[string]string{"ID": "integer", "Key": "text"}
	keys := []string{"ID", "Key"}
	values := &[]interface{}{}
	expected := `"ID"=$1::integer and "Key"=$2::text`

	if actual, err := dbbus.WherePK(PK, fieldMap, keys, values, 0); err != nil {
		t.Fatal(err)
	} else {
		if actual != expected {
			t.Fatalf("\nExpect %s\nActual %s", expected, actual)
		}
	}
}

// Case 7: one PK with nil
func Test_wherePK_result_case7(t *testing.T) {
	PK := map[string]interface{}{"ID": nil}
	fieldMap := map[string]string{"ID": "integer"}
	keys := []string{"ID"}
	values := &[]interface{}{}
	expected := `"ID" is null`

	if actual, err := dbbus.WherePK(PK, fieldMap, keys, values, 0); err != nil {
		t.Fatal(err)
	} else {
		if actual != expected {
			t.Fatalf("\nExpect %s\nActual %s", expected, actual)
		}
	}
}

// Case 8: two params in the same PK key
func Test_wherePK_result_case8(t *testing.T) {
	PK := map[string]interface{}{"ID": []int64{2, 3}}
	fieldMap := map[string]string{"ID": "integer"}
	keys := []string{"ID"}
	values := &[]interface{}{}
	expected := `("ID"=$1::integer or "ID"=$2::integer)`

	if actual, err := dbbus.WherePK(PK, fieldMap, keys, values, 0); err != nil {
		t.Fatal(err)
	} else {
		if len(*values) != 2 {
			t.Fatal("\nExpect &[2 3]\nActual", values)
		}
		if actual != expected {
			t.Fatalf("\nExpect %s\nActual %s", expected, actual)
		}
	}
}

// Case 9: one PK with boolean
func Test_wherePK_result_case9(t *testing.T) {
	PK := map[string]interface{}{"ID": true}
	fieldMap := map[string]string{"ID": "boolean"}
	keys := []string{"ID"}
	values := &[]interface{}{}
	expected := `"ID" is true`

	if actual, err := dbbus.WherePK(PK, fieldMap, keys, values, 0); err != nil {
		t.Fatal(err)
	} else {
		if actual != expected {
			t.Fatalf("\nExpect %s\nActual %s", expected, actual)
		}
	}
}

// Case 10: one PK with boolean and null
func Test_wherePK_result_case10(t *testing.T) {
	PK := map[string]interface{}{"ID": []interface{}{true, nil}}
	fieldMap := map[string]string{"ID": "boolean"}
	keys := []string{"ID"}
	values := &[]interface{}{}
	expected := `("ID" is true or "ID" is null)`

	if actual, err := dbbus.WherePK(PK, fieldMap, keys, values, 0); err != nil {
		t.Fatal(err)
	} else {
		if actual != expected {
			t.Fatalf("\nExpect %s\nActual %s", expected, actual)
		}
	}
}

// Case 11: one PK with boolean and null and other values mixed
func Test_wherePK_result_case11(t *testing.T) {
	PK := map[string]interface{}{"ID": []interface{}{true, 2, nil}, "Key": "1.2"}
	fieldMap := map[string]string{"ID": "boolean", "Key": "text"}
	keys := []string{"ID", "Key"}
	values := &[]interface{}{}
	expected := `("ID" is true or "ID"=$1::boolean or "ID" is null) and "Key"=$2::text`

	if actual, err := dbbus.WherePK(PK, fieldMap, keys, values, 0); err != nil {
		t.Fatal(err)
	} else {
		if actual != expected {
			t.Fatalf("\nExpect %s\nActual %s", expected, actual)
		}
	}
}
