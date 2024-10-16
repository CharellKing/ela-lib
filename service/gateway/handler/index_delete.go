package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type IndexDelete struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newIndexDelete(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &IndexDelete{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionDeleteIndex, newIndexDelete)
}

func (handler *IndexDelete) Run(c *gin.Context) {

}
