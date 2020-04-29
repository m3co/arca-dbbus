package dbbus

import (
	"fmt"
	"strings"
)

/* WherePK creates the string to use in the "Where" section of an SQL-query

PK       - holds the object with the values to constructh the string query with
fieldMap - describes the columns and their respective types (integer, varchar, text, boolean...)
keys     - indicates which columns from the fieldMap are considered primary keys
i        - an index for string-query-building purposes
*/
func WherePK(PK map[string]interface{}, fieldMap map[string]string, keys []string, values *[]interface{}, i int) (string, error) {
	if len(PK) == 0 {
		return "", ErrorZeroParamsInPK
	}
	if len(fieldMap) == 0 {
		return "", ErrorZeroParamsInFieldMap
	}
	if len(keys) == 0 {
		return "", ErrorZeroParamsInKeys
	}
	if values == nil {
		return "", ErrorUndefinedValuesArray
	}
	if i < 0 {
		return "", ErrorIndexNegative
	}

	condition := []string{}
	j := 0
	for _, field := range keys {
		if value, ok := PK[field]; ok {
			if value != nil {
				j++
				*values = append(*values, value)
				typefield := fieldMap[field]
				condition = append(condition, fmt.Sprintf(`"%s"=$%d::%s`,
					field, i+j, typefield))
			} else {
				condition = append(condition, fmt.Sprintf(`"%s" is null`,
					field))
			}
		}
	}
	if j == 0 {
		return "", ErrorEmptyCondition
	}
	return strings.Join(condition, " and "), nil
}
