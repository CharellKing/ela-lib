package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type DocumentUpsert struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newDocumentUpsert(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &DocumentUpsert{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionUpsertDocument, newDocumentUpsert)
}

func (handler *DocumentUpsert) Run(c *gin.Context) {

}
