package es

type UriPathParserResult struct {
	RequestAction RequestActionType
	Index         string
	IndexType     string
	DocumentId    string
}

type MatchRule struct {
	Method     string
	UriPattern string
	Priority   int
}

type ComposeRule struct {
	Method     string
	UriPattern string
}

type UriParserRule struct {
	RequestAction RequestActionType
	MatchRule     MatchRule
	ComposeRule   ComposeRule
	IsWrite       bool
}

type RequestActionType string

const (
	RequestActionTypeUpsertDocument          RequestActionType = "upsertDocument"
	RequestActionTypeCreateDocument          RequestActionType = "createDocument"
	RequestActionTypeCreateDocumentWithID                      = "createDocumentWithID"
	RequestActionTypeGetDocument             RequestActionType = "getDocument"
	RequestActionTypeDeleteDocument          RequestActionType = "deleteDocument"
	RequestActionTypeDeleteByQuery           RequestActionType = "deleteByQuery"
	RequestActionTypeUpdateDocument          RequestActionType = "updateDocument"
	RequestActionTypeUpdateByQuery           RequestActionType = "updateByQuery"
	RequestActionTypeMGetDocument            RequestActionType = "mgetDocument"
	RequestActionTypeBulkDocument            RequestActionType = "bulkDocument"
	RequestActionTypeSearchDocument          RequestActionType = "searchDocument"
	RequestActionTypeSearchDocumentWithLimit RequestActionType = "searchDocumentWithLimit"
)
