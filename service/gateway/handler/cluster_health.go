package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type ClusterHealth struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newClusterHealth(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &ClusterHealth{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionClusterHealth, newClusterHealth)
}

func (handler *ClusterHealth) Run(c *gin.Context) {
	resp, err := handler.SourceES.ClusterHealth(c)
	if err != nil {
		c.JSON(500, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(200, resp)
}