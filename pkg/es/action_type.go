package es

const (
	RequestActionGetInfo = "get-info"

	RequestActionGetDocument    = "get-document"
	RequestActionUpsertDocument = "upsert-document"
	RequestActionCreateDocument = "create-document"
	RequestActionUpdateDocument = "update-document"
	RequestActionDeleteDocument = "delete-document"
	RequestActionBulk           = "bulk-document"

	RequestActionSearch      = "search-document"
	RequestActionSearchLimit = "search-limit-document"

	RequestActionCreateIndex = "create-index"
	RequestActionGetIndex    = "get-index"
	RequestActionDeleteIndex = "delete-index"

	RequestActionClusterHealth   = "cluster-health"
	RequestActionClusterSettings = "cluster-settings"

	RequestActionGetIndexMapping    = "get-index-mapping"
	RequestActionUpdateIndexMapping = "update-index-mapping"

	RequestActionGetIndexSettings    = "get-index-settings"
	RequestActionUpdateIndexSettings = "update-index-settings"
)

func (res *UriPathParserResult) GetUri(esInstance ES) string {
	if ClusterVersionGte7(esInstance) {
		return res.UriWithoutType
	}
	return res.UriWithType
}
