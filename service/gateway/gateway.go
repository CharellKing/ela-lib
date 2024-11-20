package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"io"
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

type BulkAction struct {
	ActionType string
	Metadata   map[string]interface{}
	Document   map[string]interface{}
}

func (bulkAction *BulkAction) ToStringArray() []string {
	metadataMap := map[string]interface{}{
		bulkAction.ActionType: bulkAction.Metadata,
	}

	var bulkActionArray []string
	metadataBytes, _ := json.Marshal(metadataMap)
	bulkActionArray = append(bulkActionArray, string(metadataBytes))

	if bulkAction.Document != nil {
		documentBytes, _ := json.Marshal(bulkAction.Document)
		bulkActionArray = append(bulkActionArray, string(documentBytes))
	}

	return bulkActionArray
}

func (gateway *ESGateway) convertMasterRequestBody(masterRequestBody []byte, parserResult *es.UriPathParserResult) ([]byte, error) {
	var err error
	requestBody := masterRequestBody
	if parserResult.RequestAction == es.RequestActionTypeBulkDocument {
		var docTypeReservationType = es.DocTypeReservationTypeKeep
		if es.ClusterVersionGte7(gateway.MasterES.GetClusterVersion()) == true &&
			es.ClusterVersionGte7(gateway.SourceES.GetClusterVersion()) == false {
			docTypeReservationType = es.DocTypeReservationTypeDelete
		} else if es.ClusterVersionGte7(gateway.MasterES.GetClusterVersion()) == false &&
			es.ClusterVersionGte7(gateway.SourceES.GetClusterVersion()) == true {
			docTypeReservationType = es.DocTypeReservationTypeCreate
		}
		requestBody, err = es.AdjustBulkRequestBodyWithOnlyDocType(masterRequestBody, docTypeReservationType)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return requestBody, nil
}

func (gateway *ESGateway) convertSalveRequestBody(masterRequestBody []byte,
	masterResponse map[string]interface{}, parserResult *es.UriPathParserResult) ([]byte, error) {
	var err error
	requestBody := masterRequestBody
	if parserResult.RequestAction == es.RequestActionTypeBulkDocument {
		var docTypeReservationType = es.DocTypeReservationTypeKeep
		if es.ClusterVersionGte7(gateway.SlaveES.GetClusterVersion()) == true &&
			es.ClusterVersionGte7(gateway.SourceES.GetClusterVersion()) == false {
			docTypeReservationType = es.DocTypeReservationTypeDelete
		} else if es.ClusterVersionGte7(gateway.SlaveES.GetClusterVersion()) == false &&
			es.ClusterVersionGte7(gateway.SourceES.GetClusterVersion()) == true {
			docTypeReservationType = es.DocTypeReservationTypeCreate
		}
		requestBody, err = es.AdjustBulkRequestBody(masterRequestBody, masterResponse, docTypeReservationType)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return requestBody, nil
}

func (gateway *ESGateway) onHandler(c *gin.Context) {
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
	}

	c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	parseUriResult := gateway.SourceES.MatchRule(c)
	if parseUriResult == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("invalid uri %s", c.Request.URL.Path),
		})
		return
	}

	newBodyBytes, err := gateway.convertMasterRequestBody(bodyBytes, parseUriResult)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	resp, statusCode, err := gateway.MasterES.Request(c, newBodyBytes, parseUriResult)
	if err != nil {
		utils.GetLogger(c).Infof("master request error: %+v", err)
		c.JSON(statusCode, gin.H{
			"error": err.Error(),
		})
		return
	}
	if gateway.SlaveES.IsWrite(parseUriResult.RequestAction) && statusCode < 300 {
		utils.GoRecovery(c, func() {
			newBodyBytes, err := gateway.convertSalveRequestBody(bodyBytes, resp, parseUriResult)
			if err != nil {
				utils.GetLogger(c).Errorf("convert slave request body: %+v", err)
				return
			}
			newParseUriResult := gateway.convertSlaveMatchRule(resp, parseUriResult)
			response, status, err := gateway.SlaveES.Request(c, newBodyBytes, newParseUriResult)
			if err != nil {
				utils.GetLogger(c).Errorf("slave request error: %+v", err)
			}

			if status >= 299 {
				utils.GetLogger(c).Errorf("response: %+v, err: %+v", response, err)
			}
		})
	}

	if parseUriResult.RequestAction == es.RequestActionTypeSearchDocumentWithLimit ||
		parseUriResult.RequestAction == es.RequestActionTypeSearchDocument {
		resp = gateway.SourceES.GetSearchResponse(resp)
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
