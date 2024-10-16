package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type SettingsUpdate struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newSettingsUpdate(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &SettingsUpdate{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionUpdateIndexSettings, newSettingsUpdate)
}

func (handler *SettingsUpdate) Run(c *gin.Context) {

}
