package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type DocumentGet struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newDocumentGet(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &DocumentGet{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionDocument, newDocumentGet)
}

func (handler *DocumentGet) Run(c *gin.Context) {

}
