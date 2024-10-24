package es

type UriPathParserResult struct {
	RequestAction RequestActionType
	VariableMap   map[string]string
}

type UriPathMakeResult struct {
	Uri      string
	Method   MethodType
	Address  string
	User     string
	Password string
}

type MatchRule struct {
	Method     MethodType
	UriPattern string
	Priority   int

	RequestActionType RequestActionType
}

func newMatchRule(method MethodType, uriPattern string, priority int) *MatchRule {
	return &MatchRule{
		Method:     MethodType(method),
		UriPattern: uriPattern,
		Priority:   priority,
	}
}

type UriParserRule struct {
	MatchRules []*MatchRule
	IsWrite    bool
}

type RequestActionType string

const (
	RequestActionTypeUpsertDocument       RequestActionType = "upsertDocument"
	RequestActionTypeCreateDocument       RequestActionType = "createDocument"
	RequestActionTypeCreateDocumentWithID RequestActionType = "createDocumentWithID"
	RequestActionTypeDeleteDocument       RequestActionType = "deleteDocument"
	RequestActionTypeUpdateDocument       RequestActionType = "updateDocument"

	RequestActionTypeDeleteByQuery RequestActionType = "deleteByQuery"
	RequestActionTypeUpdateByQuery RequestActionType = "updateByQuery"

	RequestActionTypeBulkDocument RequestActionType = "bulkDocument"

	RequestActionTypeGetDocument             RequestActionType = "getDocument"
	RequestActionTypeGetDocumentOnlySource   RequestActionType = "getDocumentOnlySource"
	RequestActionTypeMGetDocument            RequestActionType = "mgetDocument"
	RequestActionTypeSearchDocument          RequestActionType = "searchDocument"
	RequestActionTypeSearchDocumentWithLimit RequestActionType = "searchDocumentWithLimit"
)
