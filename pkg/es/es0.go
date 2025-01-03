package es

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"github.com/CharellKing/ela-lib/config"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"io"
	"net/http"
	"strings"

	"github.com/pkg/errors"
)

type Operation int

const (
	OperationCreate Operation = iota
	OperationUpdate
	OperationDelete
)

type MethodType string

const (
	MethodPost   MethodType = "POST"
	MethodPut    MethodType = "PUT"
	MethodGet    MethodType = "GET"
	MethodDelete MethodType = "DELETE"
)

type ScrollResult struct {
	Total    uint64
	Docs     []*Doc
	ScrollId string
}

type Doc struct {
	Type   string                 `mapstructure:"_type" json:"_type"`
	ID     string                 `mapstructure:"_id" json:"_id"`
	Source map[string]interface{} `mapstructure:"_source" json:"_source"`
	Hash   uint64                 `mapstructure:"_hash" json:"_hash"`
	Op     Operation              `mapstructure:"_op" json:"_op"`
}

func (d *Doc) DumpFileBytes() []byte {
	buf, _ := json.Marshal(map[string]interface{}{
		"_type":   d.Type,
		"_id":     d.ID,
		"_source": d.Source,
	})
	return buf
}

type ScrollOption struct {
	Query      map[string]interface{}
	SortFields []string
	ScrollSize uint
	ScrollTime uint
	SliceId    *uint
	SliceSize  *uint
}

type ES interface {
	GetClusterVersion() string
	IndexExisted(index string) (bool, error)
	GetIndexes() ([]string, error)

	NewScroll(ctx context.Context, index string, option *ScrollOption) (*ScrollResult, error)
	NextScroll(ctx context.Context, scrollId string, scrollTime uint) (*ScrollResult, error)
	ClearScroll(scrollId string) error

	BulkBody(index string, buf *bytes.Buffer, doc *Doc) error
	Bulk(buf *bytes.Buffer) error

	GetIndexMappingAndSetting(index string) (IESSettings, error)

	CreateIndex(esSetting IESSettings) error
	DeleteIndex(index string) error

	Count(ctx context.Context, index string) (uint64, error)

	CreateTemplate(ctx context.Context, name string, body map[string]interface{}) error

	ClusterHealth(ctx context.Context) (map[string]interface{}, error)

	GetInfo(ctx context.Context) (map[string]interface{}, error)

	GetAddresses() []string

	GetUser() string

	GetPassword() string

	MatchRule(c *gin.Context) *UriPathParserResult

	MakeUri(c *gin.Context, uriPathParserResult *UriPathParserResult) (*UriPathMakeResult, error)

	GetSearchResponse(bodyMap map[string]interface{}) map[string]interface{}

	GetActionRuleMap() map[RequestActionType]*UriParserRule

	GetMethodRuleMap() map[MethodType][]*MatchRule

	IsWrite(requestActionType RequestActionType) bool

	Request(c *gin.Context, bodyBytes []byte, parserUriResult *UriPathParserResult) (map[string]interface{}, int, error)
}

type V0 struct {
	Config *config.ESConfig
}

type ClusterVersion struct {
	Name        string `json:"name,omitempty"`
	ClusterName string `json:"cluster_name,omitempty"`
	Version     struct {
		Number        string `json:"number,omitempty"`
		LuceneVersion string `json:"lucene_version,omitempty"`
	} `json:"version,omitempty"`
}

func NewESV0(config *config.ESConfig) *V0 {
	return &V0{
		Config: config,
	}
}

func (es *V0) GetES() (ES, error) {
	clusterVersion, err := es.GetVersion()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if ClusterVersionLte5(clusterVersion.Version.Number) {
		return NewESV5(es.Config, clusterVersion.Version.Number)
	} else if strings.HasPrefix(clusterVersion.Version.Number, "6.") {
		return NewESV6(es.Config, clusterVersion.Version.Number)
	} else if strings.HasPrefix(clusterVersion.Version.Number, "7.") {
		return NewESV7(es.Config, clusterVersion.Version.Number)
	} else if strings.HasPrefix(clusterVersion.Version.Number, "8.") {
		return NewESV8(es.Config, clusterVersion.Version.Number)
	}

	return nil, errors.Errorf("unsupported version: %s", clusterVersion.Version.Number)
}

func (es *V0) GetVersion() (*ClusterVersion, error) {
	byteBuf, err := es.Get(es.Config.Addresses[0])
	if err != nil {
		return nil, errors.WithStack(err)
	}

	version := &ClusterVersion{}
	err = json.Unmarshal(byteBuf, version)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return version, nil
}

func (es *V0) Get(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	transport := &http.Transport{
		DisableKeepAlives:  true,
		DisableCompression: false,
		TLSClientConfig:    &tls.Config{InsecureSkipVerify: true},
	}

	if es.Config.User != "" && es.Config.Password != "" {
		req.SetBasicAuth(es.Config.User, es.Config.Password)
	}

	client := &http.Client{Transport: transport}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("failed to get %s, status code: %d", url, resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return bodyBytes, nil
}

type IResponse interface {
	String() string
	Status() string
}

func formatError(res IResponse) error {
	statusStr := res.Status()
	bodyStr := res.String()
	return errors.Errorf("status: %s, body: %s", statusStr, bodyStr)
}

func ClusterVersionGte7(clusterVersion string) bool {
	segments := strings.Split(clusterVersion, ".")
	return cast.ToInt(segments[0]) >= 7
}

func ClusterVersionLte5(clusterVersion string) bool {
	segments := strings.Split(clusterVersion, ".")
	return cast.ToInt(segments[0]) <= 5
}
