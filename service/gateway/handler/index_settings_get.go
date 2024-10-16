package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type SettingsGet struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newSettingsGet(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &SettingsGet{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionGetIndexSettings, newSettingsGet)
}

func (handler *SettingsGet) Run(c *gin.Context) {

}
