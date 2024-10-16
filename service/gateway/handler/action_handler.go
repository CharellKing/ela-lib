package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

const (
	RequestActionUpsertDocument = "upsert-document"
	RequestActionCreateDocument = "create-document"
	RequestActionUpdateDocument = "update-document"

	RequestActionSearch              = "search-document"
	RequestActionSearchLimit         = "search-limit-document"
	RequestActionDocument            = "get-document"
	RequestActionDeleteDocument      = "delete-document"
	RequestActionBulk                = "bulk-document"
	RequestActionCreateIndex         = "create-index"
	RequestActionGetIndex            = "get-index"
	RequestActionDeleteIndex         = "delete-index"
	RequestActionClusterHealth       = "cluster-health"
	RequestActionClusterSettings     = "cluster-settings"
	RequestActionGetIndexMapping     = "get-index-mapping"
	RequestActionUpdateIndexMapping  = "update-index-mapping"
	RequestActionGetIndexSettings    = "get-index-settings"
	RequestActionUpdateIndexSettings = "update-index-settings"
)

type UriPathParserResult struct {
	RequestAction string
	Index         string
	IndexType     string
	DocumentId    string
}

type ActionHandler interface {
	Run(c *gin.Context)
}

type newActionHandler func(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler

var actionHandlerMap map[string]newActionHandler

func registerHandler(actionType string, handler newActionHandler) {
	if actionHandlerMap == nil {
		actionHandlerMap = make(map[string]newActionHandler)
	}

	actionHandlerMap[actionType] = handler
}

func GetHandler(actionType string, sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	if handler, ok := actionHandlerMap[actionType]; ok {
		return handler(sourceES, targetES, result)
	}

	return nil
}
