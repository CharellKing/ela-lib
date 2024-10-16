package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type DocumentDelete struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newDocumentDelete(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &DocumentDelete{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionDeleteDocument, newDocumentDelete)
}

func (handler *DocumentDelete) Run(c *gin.Context) {

}
