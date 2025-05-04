package es

import (
	_ "github.com/pkg/errors"
	"testing"
)

func TestMatchRule(t *testing.T) {
	baseES := BaseES{}

	var (
		ok          bool
		variableMap map[string]string
	)
	variableMap, ok = baseES.matchRule("/_search", "/${index}?/${docType}?/_search")
	if !(ok && len(variableMap) == 0) {
		t.Errorf("match failed")
		return
	}

	variableMap, ok = baseES.matchRule("/a/_search", "/${index}?/${docType}?/_search")
	if !(ok && len(variableMap) == 1 && variableMap["index"] == "a") {
		t.Errorf("variableMap: %+v", variableMap)
	}

	variableMap, ok = baseES.matchRule("/a/b/_search", "/${index}?/${docType}?/_search")
	if !(ok && len(variableMap) == 2 && variableMap["index"] == "a" && variableMap["docType"] == "b") {
		t.Errorf("match failed")
		return
	}

	variableMap, ok = baseES.matchRule("/_sarch", "/${index}?/${docType}?/_search")
	if ok {
		t.Errorf("match failed")
		return
	}

	variableMap, ok = baseES.matchRule("/_sarch", "/${index}/${docType}/${docId}")
	if ok {
		t.Errorf("match failed")
		return
	}

	variableMap, ok = baseES.matchRule("/a/_sarch", "/${index}/${docType}/${docId}")
	if ok {
		t.Errorf("match failed")
		return
	}

	variableMap, ok = baseES.matchRule("/a/b/_sarch", "/${index}/${docType}/${docId}")
	if !(ok && len(variableMap) == 3 && variableMap["index"] == "a" && variableMap["docType"] == "b" && variableMap["docId"] == "_sarch") {
		t.Errorf("match failed")
		return
	}
}
