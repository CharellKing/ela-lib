package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type MappingUpdate struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newIndexMappingUpdate(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &MappingUpdate{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionUpdateIndexMapping, newIndexMappingUpdate)
}

func (handler *MappingUpdate) Run(c *gin.Context) {

}
