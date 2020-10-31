package tool

func GetStringFromMap(m map[string]interface{}, k string) (string, bool) {
	if v, ok := m[k]; ok {
		return v.(string), ok
	}
	return "", false
}

func GetInt64FromMap(m map[string]interface{}, k string) (int64, bool) {
	if v, ok := m[k]; ok {
		return v.(int64), ok
	}
	return 0, false
}

func GetUint64FromMap(m map[string]interface{}, k string) (uint64, bool) {
	if v, ok := m[k]; ok {
		return v.(uint64), ok
	}
	return 0, false
}

func GetIntFromMap(m map[string]interface{}, k string) (int, bool) {
	if v, ok := m[k]; ok {
		return v.(int), ok
	}
	return 0, false
}

func GetUintFromMap(m map[string]interface{}, k string) (uint, bool) {
	if v, ok := m[k]; ok {
		return v.(uint), ok
	}
	return 0, false
}

func GetBoolFromMap(m map[string]interface{}, k string) (bool, bool) {
	if v, ok := m[k]; ok {
		return v.(bool), ok
	}
	return false, false
}