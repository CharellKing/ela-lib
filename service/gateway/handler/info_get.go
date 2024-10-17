package handler

import (
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/gin-gonic/gin"
)

type InfoGet struct {
	SourceES es2.ES
	TargetES es2.ES

	Result *UriPathParserResult
}

func newInfoGet(sourceES, targetES es2.ES, result *UriPathParserResult) ActionHandler {
	return &InfoGet{
		SourceES: sourceES,
		TargetES: targetES,
		Result:   result,
	}
}

func init() {
	registerHandler(RequestActionGetInfo, newInfoGet)
}

func (handler *InfoGet) Run(c *gin.Context) {
	resp, err := handler.SourceES.GetInfo(c)
	if err != nil {
		c.JSON(500, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(200, resp)
}
