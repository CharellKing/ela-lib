package es

func (es *V6) GetUriParserRuleMapWithRequestAction() map[RequestActionType]*UriParserRule {
	return map[RequestActionType]*UriParserRule{
		RequestActionTypeUpsertDocument: {
			RequestActionTypeUpsertDocument,
			MatchRule{
				"PUT",
				"/(\\w)*/(\\w)*/(\\w)*",
				999,
			},
			ComposeRule{
				"PUT",
				"/${0:n}",
			},
			true,
		},
		RequestActionTypeCreateDocumentWithID: {
			RequestActionTypeCreateDocumentWithID,
			MatchRule{
				"PUT",
				"/(\\w)*/(\\w)*/(\\w)*/_create",
				1,
			},
			ComposeRule{
				"PUT",
				"/${0:n}/_create",
			},
			true,
		},
		RequestActionTypeCreateDocument: {
			RequestActionTypeCreateDocument,
			MatchRule{

				"POST",
				"/(\\w)*/(\\w)*",
				999,
			},
			ComposeRule{
				"POST",
				"/${0:n}",
			},
			true,
		},
		RequestActionTypeGetDocument: {
			RequestActionTypeGetDocument,
			MatchRule{
				"GET",
				"/(\\w)*/(\\w)*/(\\w)*(/_source)?",
				999,
			},
			ComposeRule{
				"POST",
				"/${0:n-1}/${n-1}",
			},
			false,
		},
		RequestActionTypeDeleteDocument: {
			RequestActionTypeDeleteDocument,
			MatchRule{
				"DELETE",
				"/(\\w)*/(\\w)*/(\\w)*",
				999,
			},
			ComposeRule{
				"DELETE",
				"/${0:n}",
			},
			true,
		},
		RequestActionTypeDeleteByQuery: {
			RequestActionTypeDeleteByQuery,
			MatchRule{
				"POST",
				"/(\\w)*(/(\\w)*)?/_delete_by_query",
				1,
			},
			ComposeRule{
				"POST",
				"/${0:n}/_delete_by_query",
			},
			true,
		},
		RequestActionTypeUpdateDocument: {
			RequestActionTypeUpdateDocument,
			MatchRule{
				"POST",
				"/(\\w)*/(\\w)*/(\\w)*/_update",
				1,
			},
			ComposeRule{
				"POST",
				"/${0:n}/_update",
			},
			true,
		},
		RequestActionTypeUpdateByQuery: {
			RequestActionTypeUpdateByQuery,
			MatchRule{
				"POST",
				"/(\\w)*(/(\\w)*)?/_update_by_query",
				1,
			},
			ComposeRule{
				"POST",
				"/${0:n}/_update_by_query",
			},
			true,
		},
		RequestActionTypeMGetDocument: {
			RequestActionTypeMGetDocument,
			MatchRule{
				"GET",
				"/(\\w)*(/(\\w)*)?/_mget",
				1,
			},
			ComposeRule{
				"GET",
				"/${0:n}/_mget",
			},
			true,
		},
		RequestActionTypeBulkDocument: {
			RequestActionTypeMGetDocument,
			MatchRule{
				"POST",
				"/_bulk",
				1,
			},
			ComposeRule{
				"GET",
				"/_bulk",
			},
			true,
		},
		RequestActionTypeSearchDocument: {
			RequestActionTypeSearchDocument,
			MatchRule{
				"GET",
				"(/(\\w)*)?(/(\\w)*)?/_search",
				1,
			},
			ComposeRule{
				"GET",
				"/${0:n}/_search",
			},
			true,
		},
		RequestActionTypeSearchDocumentWithLimit: {
			RequestActionTypeSearchDocument,
			MatchRule{
				"POST",
				"(/(\\w)*)?(/(\\w)*)?/_search",
				1,
			},
			ComposeRule{
				"POST",
				"/${0:n}/_search",
			},
			true,
		},
	}
}
