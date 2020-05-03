package dbbus

import (
	"fmt"
	"reflect"
	"strings"
)

func interfaceSlice(slice interface{}) []interface{} {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		panic("InterfaceSlice() given a non-slice type")
	}

	ret := make([]interface{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	return ret
}

// WherePK creates the string to use in the "Where" section of an SQL-query
//
// PK       - holds the object with the values to constructh the string query with.
// fieldMap - describes the columns and their respective types:
//              integer, varchar, text, boolean
// keys     - indicates which columns from the fieldMap are considered primary keys.
// values   - points to the array that holds the values to be rendered in the query
// i        - an index for string-query-building purposes.
func WherePK(
	PK map[string]interface{},
	fieldMap map[string]string,
	keys []string,
	values *[]interface{},
	i int) (string, error) {
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
			typefield := fieldMap[field]
			if typefield == "" {
				return "", ErrorKeyNotInFieldMap
			}
			if value != nil {
				t := reflect.TypeOf(value).Kind()
				if (t == reflect.Array) || (t == reflect.Slice) {
					ors := []string{}
					for _, value := range interfaceSlice(value) {
						if value != nil {
							t := reflect.TypeOf(value).Kind()
							if t == reflect.Bool {
								v, ok := value.(bool)
								if ok {
									if v {
										ors = append(ors, fmt.Sprintf(`"%s" is true`,
											field))
									} else {
										ors = append(ors, fmt.Sprintf(`"%s" is false`,
											field))
									}
								} else {
									return "", ErrorCastToBool
								}
							} else {
								j++
								*values = append(*values, value)
								ors = append(ors, fmt.Sprintf(`"%s"=$%d::%s`,
									field, i+j, typefield))
							}
						} else {
							ors = append(ors, fmt.Sprintf(`"%s" is null`,
								field))
						}
					}
					condition = append(condition, fmt.Sprintf("(%s)", strings.Join(ors, " or ")))
				} else {
					t := reflect.TypeOf(value).Kind()
					if t == reflect.Bool {
						v, ok := value.(bool)
						if ok {
							if v {
								condition = append(condition, fmt.Sprintf(`"%s" is true`,
									field))
							} else {
								condition = append(condition, fmt.Sprintf(`"%s" is false`,
									field))
							}
						} else {
							return "", ErrorCastToBool
						}
					} else if t == reflect.String {
						str, ok := value.(string)
						if ok {
							hasStart := false
							if str[:1] == "%" {
								hasStart = true
								str = str[1:]
							}
							hasEnd := false
							if str[len(str)-1:] == "%" {
								hasEnd = true
								str = str[:len(str)-1]
							}
							j++
							if hasEnd || hasStart {
								*values = append(*values, str)
								likeStart := ""
								if hasStart {
									likeStart = "'%' ||"
								}
								likeEnd := ""
								if hasEnd {
									likeEnd = "|| '%'"
								}
								like := fmt.Sprintf(`"%s" like %s $%d::%s %s`,
									field, likeStart, i+j, typefield, likeEnd)
								condition = append(condition, like)
							} else {
								*values = append(*values, value)
								condition = append(condition, fmt.Sprintf(`"%s"=$%d::%s`,
									field, i+j, typefield))
							}
						}
					} else {
						j++
						*values = append(*values, value)
						condition = append(condition, fmt.Sprintf(`"%s"=$%d::%s`,
							field, i+j, typefield))
					}
				}
			} else {
				condition = append(condition, fmt.Sprintf(`"%s" is null`,
					field))
			}
		}
	}
	if len(condition) == 0 {
		return "", ErrorEmptyCondition
	}
	return strings.Join(condition, " and "), nil
}
