package gateway

import (
	"encoding/json"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/service/gateway/handler"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
	"io"

	"math/rand"
	"net/http"
	"net/url"
	"strings"
)

type ESGateway struct {
	Engine   *gin.Engine
	Address  string
	User     string
	Password string

	SourceES es.ES
	TargetES es.ES

	MasterES es.ES
	SlaveES  es.ES
}

func basicAuth(username, password string) gin.HandlerFunc {
	return func(c *gin.Context) {
		user, pass, hasAuth := c.Request.BasicAuth()
		if hasAuth && user == username && pass == password {
			c.Next()
		} else {
			c.Header("WWW-Authenticate", `Basic realm="Restricted"`)
			c.AbortWithStatus(http.StatusUnauthorized)
		}
	}
}

func NewESGateway(cfg *config.Config) (*ESGateway, error) {
	engine := gin.Default()
	engine.Use(basicAuth(cfg.GatewayCfg.User, cfg.GatewayCfg.Password))

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

	masterES := sourceES
	slaveES := targetES
	if cfg.GatewayCfg.Master == cfg.GatewayCfg.TargetES {
		masterES = targetES
		slaveES = sourceES
	}

	return &ESGateway{
		Engine:   engine,
		Address:  cfg.GatewayCfg.Address,
		User:     cfg.GatewayCfg.User,
		Password: cfg.GatewayCfg.Password,

		SourceES: sourceES,
		TargetES: targetES,
		MasterES: masterES,
		SlaveES:  slaveES,
	}, nil
}

func (gateway *ESGateway) parseUriPath(httpAction, uriPath string, needType bool) (*handler.UriPathParserResult, error) {
	var (
		result handler.UriPathParserResult
	)

	result.HttpAction = httpAction
	result.Uri = uriPath

	if uriPath == "/" && httpAction == "GET" {
		result.RequestAction = handler.RequestActionGetInfo
		return &result, nil
	}

	segments := strings.Split(uriPath, "/")
	if strings.HasSuffix("/_create", uriPath) {
		if httpAction == "POST" {
			result.RequestAction = handler.RequestActionCreateDocument
		}
		result.Index = segments[1]
		if len(segments) == 5 {
			result.IndexType = segments[2]
			result.DocumentId = segments[3]
			if !needType {
				result.Uri = strings.Join(append(segments[:2], segments[3:]...), "/")
			}
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
			if !needType {
				result.Uri = strings.Join(append(segments[:2], segments[3:]...), "/")
			}
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
			if !needType {
				result.Uri = strings.Join(append(segments[:2], segments[3:]...), "/")
			}
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
			if !needType {
				result.Uri = strings.Join(append(segments[:2], segments[3:]...), "/")
			}
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
			if !needType {
				result.Uri = strings.Join(append(segments[:2], segments[3:]...), "/")
			}
		} else if len(segments) == 3 {
			result.DocumentId = segments[2]
		}
		return &result, nil
	}
	return &result, fmt.Errorf("invalid uri %s", uriPath)
}

func (gateway *ESGateway) onHandler(c *gin.Context) {
	utils.GoRecovery(c, func() {
		_, _, _, err := gateway.proxy(c, gateway.SlaveES)
		if err != nil {
			utils.GetLogger(c).Error("salve request %+v", err)
		}
	})

	header, body, statusCode, err := gateway.proxy(c, gateway.MasterES)
	// 将 header, body, statusCode 返回给客户端
	if err != nil {
		c.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}

	for key, values := range header {
		for _, value := range values {
			c.Header(key, value)
		}
	}

	bodyMap := gjson.ParseBytes(body).Value().(map[string]interface{})

	if gateway.MasterES != gateway.SourceES && statusCode == http.StatusOK {
		if es.ClusterVersionGte7(gateway.MasterES) && !es.ClusterVersionGte7(gateway.SourceES) {
			totalValue, _ := utils.GetValueFromMapByPath(bodyMap, "hits.total.value")
			utils.SetValueFromMapByPath(bodyMap, "hit.total", totalValue)
		}

		if !es.ClusterVersionGte7(gateway.MasterES) && es.ClusterVersionGte7(gateway.SourceES) {
			totalValue, _ := utils.GetValueFromMapByPath(bodyMap, "hits.total")
			utils.SetValueFromMapByPath(bodyMap, "hit.total", map[string]interface{}{
				"value":    totalValue,
				"relation": "eq",
			})
		}
	}

	body, _ = json.Marshal(bodyMap)
	c.JSON(statusCode, body)
}

func (gateway *ESGateway) onRequestGet() {
	gateway.Engine.GET("/*path", func(c *gin.Context) {
		gateway.onHandler(c)
	})
}

func (gateway *ESGateway) onRequestPost() {
	gateway.Engine.POST("/*path", func(c *gin.Context) {
		// 处理 POST 请求
		gateway.onHandler(c)
	})
}

func (gateway *ESGateway) onRequestPut() {
	gateway.Engine.PUT("/*path", func(c *gin.Context) {
		// 处理 POST 请求
		gateway.onHandler(c)
	})
}

func (gateway *ESGateway) onRequestDelete() {
	gateway.Engine.DELETE("/*path", func(c *gin.Context) {
		// 处理 POST 请求
		gateway.onHandler(c)
	})
}

func (gateway *ESGateway) Run() {
	gateway.onRequestGet()
	gateway.onRequestPost()
	gateway.onRequestPut()
	gateway.onRequestDelete()

	_ = gateway.Engine.Run(gateway.Address)
}

func (gateway *ESGateway) proxy(c *gin.Context, esInstance es.ES) (header http.Header, body []byte, statusCode int, err error) {
	addresses := esInstance.GetAddresses()
	address := addresses[rand.Intn(len(addresses))]

	var needType bool
	clusterVersion := esInstance.GetClusterVersion()
	if strings.HasPrefix(clusterVersion, "5.") || strings.HasPrefix(clusterVersion, "6.") {
		needType = true
	}

	uriParserResult, err := gateway.parseUriPath(c.Request.Method, c.Request.URL.Path, needType)
	if err != nil {
		return nil, nil, 0, errors.WithStack(err)
	}

	proxy, err := url.Parse(address)
	if err != nil {
		return nil, nil, 0, errors.WithStack(err)
	}

	proxyURL := proxy.ResolveReference(&url.URL{Path: uriParserResult.Uri})
	req, err := http.NewRequest(c.Request.Method, proxyURL.String(), c.Request.Body)
	if err != nil {
		return nil, nil, 0, errors.WithStack(err)
	}

	for key, values := range c.Request.Header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	req.SetBasicAuth(esInstance.GetUser(), esInstance.GetPassword())

	// 执行请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, 0, errors.WithStack(err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	header = make(http.Header)
	for key, values := range resp.Header {
		for _, value := range values {
			if value == "" {
				header.Del(key)
			}
			header.Set(key, value)
		}
	}

	statusCode = resp.StatusCode
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, 0, errors.WithStack(err)
	}

	return header, body, statusCode, nil
}
