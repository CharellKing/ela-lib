package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type MappingGet struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newIndexMappingGet(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &MappingGet{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionGetIndexMapping, newIndexMappingGet)
}

func (handler *MappingGet) Run(c *gin.Context) {

}
