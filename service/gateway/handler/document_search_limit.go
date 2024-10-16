package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type DocumentSearchLimit struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newDocumentSearchLimit(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &DocumentSearchLimit{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionSearchLimit, newDocumentSearchLimit)
}

func (handler *DocumentSearchLimit) Run(c *gin.Context) {

}
