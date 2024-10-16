package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type DocumentBulk struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newDocumentBulk(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &DocumentBulk{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionBulk, newDocumentBulk)
}

func (handler *DocumentBulk) Run(c *gin.Context) {

}
