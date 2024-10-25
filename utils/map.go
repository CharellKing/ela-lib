package utils

import (
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"strings"
)

func GetValueFromMapByPath(data map[string]interface{}, path string) (interface{}, bool) {
	if lo.IsEmpty(path) {
		return nil, false
	}
	keys := strings.Split(path, ".")
	value := data
	for _, key := range keys {
		v, ok := value[key]
		if !ok {
			return nil, false
		}
		value = v.(map[string]interface{})
	}
	return value, true
}

func SetValueFromMapByPath(data map[string]interface{}, path string, value interface{}) bool {
	if path == "" {
		return false
	}
	keys := strings.Split(path, ".")
	lastKeyIndex := len(keys) - 1
	for i, key := range keys {
		if i == lastKeyIndex {
			data[key] = value
			return true
		}
		if _, ok := data[key]; !ok {
			data[key] = make(map[string]interface{})
		}
		var ok bool
		data, ok = data[key].(map[string]interface{})
		if !ok {
			return false
		}
	}
	return false
}

func GetFirstKeyMapValue(m map[string]interface{}) (string, map[string]interface{}) {
	for k, v := range m {
		return k, cast.ToStringMap(v)
	}
	return "", nil
}
