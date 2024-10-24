package gateway

import (
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"net/http"
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

func (gateway *ESGateway) convertSlaveMatchRule(masterResponse map[string]interface{}, parseResult *es.UriPathParserResult) *es.UriPathParserResult {
	if parseResult.RequestAction == es.RequestActionTypeCreateDocument {
		documentId := cast.ToString(masterResponse["_id"])
		return &es.UriPathParserResult{
			VariableMap: lo.Assign(parseResult.VariableMap, map[string]string{
				"docId": documentId,
			}),
			RequestAction: es.RequestActionTypeUpsertDocument,
		}
	}
	return parseResult
}

func (gateway *ESGateway) onHandler(c *gin.Context) {
	parseUriResult := gateway.SourceES.MatchRule(c)
	if parseUriResult == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("invalid uri %s", c.Request.URL.Path),
		})
		return
	}
	resp, statusCode, err := gateway.MasterES.Request(c, parseUriResult)
	if err != nil {
		utils.GetLogger(c).Infof("master request error: %+v", err)
		c.JSON(statusCode, gin.H{
			"error": err.Error(),
		})
		return
	}
	if gateway.SlaveES.IsWrite(parseUriResult.RequestAction) {
		utils.GoRecovery(c, func() {
			newParseUriResult := gateway.convertSlaveMatchRule(resp, parseUriResult)
			_, _, err = gateway.SlaveES.Request(c, newParseUriResult)
			if err != nil {
				utils.GetLogger(c).Infof("slave request error: %+v", err)
			}
		})
	}

	c.JSON(statusCode, resp)
}

func (gateway *ESGateway) onRequest() {
	gateway.Engine.NoRoute(func(c *gin.Context) {
		gateway.onHandler(c)
	})
}

func (gateway *ESGateway) Run() {
	gateway.onRequest()

	_ = gateway.Engine.Run(gateway.Address)
}
