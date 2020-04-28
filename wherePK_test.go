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
