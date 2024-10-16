package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type IndexGet struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newIndexGet(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &IndexGet{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionGetIndex, newIndexGet)
}

func (handler *IndexGet) Run(c *gin.Context) {

}
