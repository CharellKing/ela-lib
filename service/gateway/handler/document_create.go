package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type DocumentCreate struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newDocumentCreate(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &DocumentCreate{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionCreateDocument, newDocumentCreate)
}

func (handler *DocumentCreate) Run(c *gin.Context) {

}
