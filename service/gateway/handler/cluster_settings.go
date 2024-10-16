package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type ClusterSettings struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newClusterSettings(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &ClusterSettings{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionClusterSettings, newClusterSettings)
}

func (handler *ClusterSettings) Run(c *gin.Context) {

}
