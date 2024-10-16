package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type DocumentSearch struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newDocumentSearch(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &DocumentSearch{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionSearch, newDocumentSearch)
}

func (handler *DocumentSearch) Run(c *gin.Context) {

}
