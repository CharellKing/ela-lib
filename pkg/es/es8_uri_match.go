package es

func getUriParserRuleMapWithRequestActionForV8() map[RequestActionType]*UriParserRule {
	return map[RequestActionType]*UriParserRule{
		RequestActionTypeUpsertDocument: {
			[]*MatchRule{
				newMatchRule(MethodPut, "/${index}/_doc/${docId}", 1),
			},
			true,
		},
		RequestActionTypeCreateDocumentWithID: {
			[]*MatchRule{
				newMatchRule(MethodPut, "/${index}/_create/${docId}", 1),
				newMatchRule(MethodPost, "/${index}/_create/${docId}", 1),
			},
			true,
		},
		RequestActionTypeCreateDocument: {
			[]*MatchRule{
				newMatchRule(MethodPost, "/${index}/_doc", 1),
			},
			true,
		},
		RequestActionTypeGetDocument: {
			[]*MatchRule{
				newMatchRule(MethodGet, "/${index}/_doc/${docId}", 1),
			},
			false,
		},
		RequestActionTypeGetDocumentOnlySource: {
			[]*MatchRule{
				newMatchRule(MethodGet, "/${index}/_source/${docId}", 1),
			},
			false,
		},
		RequestActionTypeDeleteDocument: {
			[]*MatchRule{
				newMatchRule(MethodDelete, "/${index}/_doc/${docId}", 999),
			},
			true,
		},
		RequestActionTypeDeleteByQuery: {
			[]*MatchRule{
				newMatchRule(MethodPost, "/${index}/_delete_by_query", 1),
			},
			true,
		},
		RequestActionTypeUpdateDocument: {
			[]*MatchRule{
				newMatchRule(MethodPost, "/${index}/_update/${docId}", 1),
			},
			true,
		},
		RequestActionTypeUpdateByQuery: {
			[]*MatchRule{
				newMatchRule(MethodPost, "/${index}/_update_by_query", 1),
			},
			true,
		},
		RequestActionTypeMGetDocument: {
			[]*MatchRule{
				newMatchRule(MethodGet, "/${index}?/_mget", 1),
			},
			true,
		},
		RequestActionTypeBulkDocument: {
			[]*MatchRule{
				newMatchRule(MethodPost, "/${index}?/_bulk", 1),
			},
			true,
		},
		RequestActionTypeSearchDocument: {
			[]*MatchRule{
				newMatchRule(MethodGet, "/${index}?/_search", 1),
			},
			true,
		},
		RequestActionTypeSearchDocumentWithLimit: {
			[]*MatchRule{
				newMatchRule(MethodPost, "/${index}?/_search", 1),
			},
			true,
		},
	}
}
