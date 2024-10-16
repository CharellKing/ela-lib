package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type DocumentUpdate struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newDocumentUpdate(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &DocumentUpdate{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionUpdateDocument, newDocumentUpdate)
}

func (handler *DocumentUpdate) Run(c *gin.Context) {

}
