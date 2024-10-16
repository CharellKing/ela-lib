package gateway

import (
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/service/gateway/handler"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"strings"
)

type ESGateway struct {
	Engine  *gin.Engine
	Address string

	SourceES es.ES
	TargetES es.ES
}

func NewESGateway(cfg *config.Config) (*ESGateway, error) {
	engine := gin.Default()

	var (
		sourceES es.ES
		targetES es.ES
		err      error
	)

	if sourceES, err = es.NewESV0(cfg.ESConfigs[cfg.GatewayCfg.SourceES]).GetES(); err != nil {
		return nil, errors.WithStack(err)
	}

	if targetES, err = es.NewESV0(cfg.ESConfigs[cfg.GatewayCfg.TargetES]).GetES(); err != nil {
		return nil, errors.WithStack(err)
	}

	return &ESGateway{
		Engine:   engine,
		Address:  cfg.GatewayCfg.Address,
		SourceES: sourceES,
		TargetES: targetES,
	}, nil
}

func (proxy *ESGateway) parseUriPath(httpAction, uriPath string) (*handler.UriPathParserResult, error) {
	var (
		result handler.UriPathParserResult
	)

	segments := strings.Split(uriPath, "/")
	if strings.HasSuffix("/_create", uriPath) {
		if httpAction == "POST" {
			result.RequestAction = handler.RequestActionCreateDocument
		}
		result.Index = segments[1]
		if len(segments) == 5 {
			result.IndexType = segments[2]
			result.DocumentId = segments[3]
		} else if len(segments) == 4 {
			result.DocumentId = segments[2]
		}
		return &result, nil
	}

	if strings.HasSuffix("/_update", uriPath) {
		if httpAction == "POST" {
			result.RequestAction = handler.RequestActionUpdateDocument
		}

		result.Index = segments[1]
		if len(segments) == 5 {
			result.IndexType = segments[2]
			result.DocumentId = segments[3]
		} else if len(segments) == 4 {
			result.DocumentId = segments[2]
		}
		return &result, nil
	}

	if strings.HasSuffix("/_search", uriPath) {
		if httpAction == "GET" {
			result.RequestAction = handler.RequestActionSearch
		}

		if httpAction == "POST" {
			result.RequestAction = handler.RequestActionSearchLimit
		}

		result.Index = segments[1]
		if len(segments) == 4 {
			result.IndexType = segments[2]
		}
		return &result, nil
	}

	if uriPath == "/_cluster/health" {
		if httpAction == "GET" {
			result.RequestAction = handler.RequestActionClusterHealth
		}
		return &result, nil
	}

	if uriPath == "/_cluster/settings" {
		if httpAction == "GET" {
			result.RequestAction = handler.RequestActionClusterSettings
		}
		return &result, nil
	}

	if strings.HasSuffix(uriPath, "/_mapping") {
		if httpAction == "GET" {
			result.RequestAction = handler.RequestActionGetIndexMapping
		}

		if httpAction == "PUT" {
			result.RequestAction = handler.RequestActionUpdateIndexMapping
		}

		result.Index = segments[1]
		if len(segments) == 4 {
			result.IndexType = segments[2]
		}

		return &result, nil

	}

	if strings.HasSuffix(uriPath, "/_settings") {
		if httpAction == "GET" {
			result.RequestAction = handler.RequestActionGetIndexSettings
		}

		if httpAction == "PUT" {
			result.RequestAction = handler.RequestActionUpdateIndexSettings
		}

		result.Index = segments[1]

		return &result, nil
	}

	if strings.HasSuffix(uriPath, "/_bulk") {
		result.RequestAction = handler.RequestActionBulk

		return &result, nil
	}

	if len(segments) == 2 {
		if httpAction == "GET" {
			result.RequestAction = handler.RequestActionGetIndex
		}

		if httpAction == "PUT" {
			result.RequestAction = handler.RequestActionCreateIndex
		}

		if httpAction == "DELETE" {
			result.RequestAction = handler.RequestActionDeleteIndex
		}

		result.Index = segments[1]
		return &result, nil
	}

	if len(segments) >= 3 {
		if httpAction == "GET" {
			result.RequestAction = handler.RequestActionDocument
		}

		if httpAction == "PUT" {
			result.RequestAction = handler.RequestActionUpsertDocument
		}

		if httpAction == "DELETE" {
			result.RequestAction = handler.RequestActionDeleteDocument
		}

		result.Index = segments[1]
		if len(segments) == 4 {
			result.IndexType = segments[2]
			result.DocumentId = segments[3]
		} else if len(segments) == 3 {
			result.DocumentId = segments[2]
		}
		return &result, nil
	}
	return &result, fmt.Errorf("invalid uri %s", uriPath)
}

func (proxy *ESGateway) onHandler(c *gin.Context, parseUriResult *handler.UriPathParserResult) {
	actionHandler := handler.GetHandler(parseUriResult.RequestAction, proxy.SourceES, proxy.TargetES, parseUriResult)
	actionHandler.Run(c)
}

func (proxy *ESGateway) onRequestGet() {
	proxy.Engine.GET("/*path", func(c *gin.Context) {
		parseUriResult, err := proxy.parseUriPath("GET", c.Request.URL.Path)
		if err != nil {
			c.JSON(400, gin.H{
				"message": err.Error(),
			})
			return
		}
		proxy.onHandler(c, parseUriResult)
	})
}

func (proxy *ESGateway) onRequestPost() {
	proxy.Engine.POST("/*path", func(c *gin.Context) {
		// 处理 POST 请求
		parseUriResult, err := proxy.parseUriPath("POST", c.Request.URL.Path)
		if err != nil {
			c.JSON(400, gin.H{
				"message": err.Error(),
			})
			return
		}
		proxy.onHandler(c, parseUriResult)
	})
}

func (proxy *ESGateway) onRequestPut() {
	proxy.Engine.PUT("/*path", func(c *gin.Context) {
		// 处理 POST 请求
		parseUriResult, err := proxy.parseUriPath("PUT", c.Request.URL.Path)
		if err != nil {
			c.JSON(400, gin.H{
				"message": err.Error(),
			})
			return
		}
		proxy.onHandler(c, parseUriResult)
	})
}

func (proxy *ESGateway) onRequestDelete() {
	proxy.Engine.DELETE("/*path", func(c *gin.Context) {
		// 处理 POST 请求
		parseUriResult, err := proxy.parseUriPath("DELETE", c.Request.URL.Path)
		if err != nil {
			c.JSON(400, gin.H{
				"message": err.Error(),
			})
			return
		}
		proxy.onHandler(c, parseUriResult)
	})
}

func (proxy *ESGateway) Run() {
	proxy.onRequestGet()
	proxy.onRequestPost()
	proxy.onRequestPut()
	proxy.onRequestDelete()

	_ = proxy.Engine.Run(proxy.Address)
}
