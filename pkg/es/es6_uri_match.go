package es

func getUriParserRuleMapWithRequestActionForV6() map[RequestActionType]*UriParserRule {
	return map[RequestActionType]*UriParserRule{
		RequestActionTypeUpsertDocument: {
			[]*MatchRule{
				newMatchRule(MethodPut, "/${index}/${docType}/${docId}", 999),
			},
			true,
		},
		RequestActionTypeCreateDocumentWithID: {
			[]*MatchRule{
				newMatchRule(MethodPut, "/${index}/${docType}/${docId}/_create", 1),
			},
			true,
		},
		RequestActionTypeCreateDocument: {
			[]*MatchRule{
				newMatchRule(MethodPost, "/${index}/${docType}", 999),
			},
			true,
		},
		RequestActionTypeGetDocument: {
			[]*MatchRule{
				newMatchRule(MethodGet, "/${index}/${docType}/${docId}", 999),
			},
			false,
		},
		RequestActionTypeGetDocumentOnlySource: {
			[]*MatchRule{
				newMatchRule(MethodGet, "/${index}/${docType}/${docId}/_source", 1),
			},
			false,
		},
		RequestActionTypeDeleteDocument: {
			[]*MatchRule{
				newMatchRule(MethodDelete, "/${index}/${docType}/${docId}", 999),
			},
			true,
		},
		RequestActionTypeDeleteByQuery: {
			[]*MatchRule{
				newMatchRule(MethodPost, "/${index}/${docType}/_delete_by_query", 1),
			},
			true,
		},
		RequestActionTypeUpdateDocument: {
			[]*MatchRule{
				newMatchRule(MethodPost, "/${index}/${docType}/${docId}/_update", 1),
			},
			true,
		},
		RequestActionTypeUpdateByQuery: {
			[]*MatchRule{
				newMatchRule(MethodPost, "/${index}/${docType}/_update_by_query", 1),
			},
			true,
		},
		RequestActionTypeMGetDocument: {
			[]*MatchRule{
				newMatchRule(MethodGet, "/${index}?/${docType}?/_mget", 1),
			},
			false,
		},
		RequestActionTypeBulkDocument: {
			[]*MatchRule{
				newMatchRule(MethodPost, "/${index}?/_bulk", 1),
			},
			true,
		},
		RequestActionTypeSearchDocument: {
			[]*MatchRule{
				newMatchRule(MethodGet, "/${index}/${docType}?/_search", 1),
			},
			false,
		},
		RequestActionTypeSearchDocumentWithLimit: {
			[]*MatchRule{
				newMatchRule(MethodGet, "/${index}/${docType}?/_search?size=${size}", 1),
			},
			false,
		},
	}
}
