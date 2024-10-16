package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type IndexCreate struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newIndexCreate(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &IndexCreate{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionCreateIndex, newIndexCreate)
}

func (handler *IndexCreate) Run(c *gin.Context) {

}
