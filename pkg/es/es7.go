package es

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/mitchellh/mapstructure"
	lop "github.com/samber/lo/parallel"
	"github.com/spf13/cast"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

type V7 struct {
	*elasticsearch7.Client
	*BaseES
}

func NewESV7(esConfig *config.ESConfig, clusterVersion string) (*V7, error) {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	client, err := elasticsearch7.NewClient(elasticsearch7.Config{
		Addresses: esConfig.Addresses,
		Username:  esConfig.User,
		Password:  esConfig.Password,
		Transport: transport,
	})

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &V7{
		Client: client,
		BaseES: NewBaseES(clusterVersion, esConfig.Addresses, esConfig.User, esConfig.Password),
	}, nil
}

func (es *V7) GetClusterVersion() string {
	return es.ClusterVersion
}

type ScrollResultV7 struct {
	Took     int    `json:"took,omitempty"`
	ScrollId string `json:"_scroll_id,omitempty"`
	TimedOut bool   `json:"timed_out,omitempty"`
	Hits     struct {
		MaxScore float32 `json:"max_score,omitempty"`
		Total    struct {
			Value    int    `json:"value,omitempty"`
			Relation string `json:"relation,omitempty"`
		} `json:"total,omitempty"`
		Docs []interface{} `json:"hits,omitempty"`
	} `json:"hits"`
	Shards struct {
		Successful int `json:"successful,omitempty"`
		Skipped    int `json:"skipped,omitempty"`
		Failed     int `json:"failed,omitempty"`
		Failures   []struct {
			Shard  int         `json:"shard,omitempty"`
			Index  string      `json:"index,omitempty"`
			Status int         `json:"status,omitempty"`
			Reason interface{} `json:"reason,omitempty"`
		} `json:"failures,omitempty"`
	} `json:"_shards,omitempty"`
}

func (es *V7) NewScroll(ctx context.Context, index string, option *ScrollOption) (*ScrollResult, error) {
	scrollSearchOptions := []func(*esapi.SearchRequest){
		es.Search.WithIndex(index),
		es.Search.WithSize(cast.ToInt(option.ScrollSize)),
		es.Search.WithScroll(cast.ToDuration(option.ScrollTime) * time.Minute),
	}

	query := make(map[string]interface{})
	for k, v := range option.Query {
		query[k] = v
	}

	if option.SliceId != nil {
		query["slice"] = map[string]interface{}{
			"field": "_id",
			"id":    *option.SliceId,
			"max":   *option.SliceSize,
		}
	}

	if len(query) > 0 {
		var buf bytes.Buffer
		_ = json.NewEncoder(&buf).Encode(query)
		scrollSearchOptions = append(scrollSearchOptions, es.Client.Search.WithBody(&buf))
	}

	if len(option.SortFields) > 0 {
		scrollSearchOptions = append(scrollSearchOptions, es.Client.Search.WithSort(option.SortFields...))
	}

	res, err := es.Client.Search(scrollSearchOptions...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.IsError() {
		return nil, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()
	var scrollResult ScrollResultV7
	if err := json.NewDecoder(res.Body).Decode(&scrollResult); err != nil {
		return nil, errors.WithStack(err)
	}

	hitDocs := lop.Map(scrollResult.Hits.Docs, func(hit interface{}, _ int) *Doc {
		var hitDoc Doc
		_ = mapstructure.Decode(hit, &hitDoc)
		return &hitDoc
	})

	return &ScrollResult{
		Total:    uint64(scrollResult.Hits.Total.Value),
		Docs:     hitDocs,
		ScrollId: scrollResult.ScrollId,
	}, nil
}

func (es *V7) NextScroll(ctx context.Context, scrollId string, scrollTime uint) (*ScrollResult, error) {
	res, err := es.Client.Scroll(es.Client.Scroll.WithScrollID(scrollId), es.Client.Scroll.WithScroll(time.Duration(scrollTime)*time.Minute))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.IsError() {
		return nil, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	var scrollResult ScrollResultV7
	if err := json.NewDecoder(res.Body).Decode(&scrollResult); err != nil {
		return nil, errors.WithStack(err)
	}

	hitDocs := lop.Map(scrollResult.Hits.Docs, func(hit interface{}, _ int) *Doc {
		var hitDoc Doc
		_ = mapstructure.Decode(hit, &hitDoc)
		return &hitDoc
	})

	return &ScrollResult{
		Total:    uint64(scrollResult.Hits.Total.Value),
		Docs:     hitDocs,
		ScrollId: scrollResult.ScrollId,
	}, nil
}

func (es *V7) ClearScroll(scrollId string) error {
	res, err := es.Client.ClearScroll(es.Client.ClearScroll.WithScrollID(scrollId))
	if err != nil {
		return errors.WithStack(err)
	}

	if res.IsError() {
		return formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	return nil
}

func (es *V7) GetIndexMappingAndSetting(index string) (IESSettings, error) {
	// Get settings
	exists, err := es.IndexExisted(index)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists {
		return nil, nil
	}

	setting, err := es.GetIndexSettings(index)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	mapping, err := es.GetIndexMapping(index)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	aliases, err := es.GetIndexAliases(index)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return NewV7Settings(setting, mapping, aliases, index), nil
}

func (es *V7) GetIndexAliases(index string) (map[string]interface{}, error) {
	// Get alias configuration
	res, err := es.Client.Indices.GetAlias(es.Client.Indices.GetAlias.WithIndex(index))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.IsError() {
		return nil, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	indexAliases := make(map[string]interface{})
	if err := json.Unmarshal(bodyBytes, &indexAliases); err != nil {
		return nil, errors.WithStack(err)
	}
	return indexAliases, nil
}

func (es *V7) GetIndexMapping(index string) (map[string]interface{}, error) {
	// Get settings
	res, err := es.Client.Indices.GetMapping(es.Client.Indices.GetMapping.WithIndex(index))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.IsError() {
		return nil, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	indexMapping := make(map[string]interface{})
	if err := json.Unmarshal(bodyBytes, &indexMapping); err != nil {
		return nil, errors.WithStack(err)
	}
	return indexMapping, nil
}

func (es *V7) GetIndexSettings(index string) (map[string]interface{}, error) {
	// Get settings
	res, err := es.Client.Indices.GetSettings(es.Client.Indices.GetSettings.WithIndex(index))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.IsError() {
		return nil, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	var indexSetting map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&indexSetting); err != nil {
		return nil, errors.WithStack(err)
	}

	return indexSetting, nil
}

func (es *V7) BulkBody(index string, buf *bytes.Buffer, doc *Doc) error {
	action := ""
	var body map[string]interface{}

	switch doc.Op {
	case OperationCreate:
		action = "index"
		body = doc.Source
	case OperationUpdate:
		action = "update"
		body = map[string]interface{}{
			doc.Type: doc.Source,
		}
	case OperationDelete:
		action = "delete"
	default:
		return fmt.Errorf("unknow action %+v", doc.Op)
	}

	meta := map[string]interface{}{
		action: map[string]interface{}{
			"_index": index,
			"_id":    doc.ID,
		},
	}

	metaBytes, _ := json.Marshal(meta)
	buf.Write(metaBytes)
	buf.WriteByte('\n')

	if len(body) > 0 {
		dataBytes, _ := json.Marshal(body)
		buf.Write(dataBytes)
		buf.WriteByte('\n')
	}
	return nil
}

func (es *V7) Bulk(buf *bytes.Buffer) error {
	// Execute the bulk request
	res, err := es.Client.Bulk(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return errors.WithStack(err)
	}

	if res.IsError() {
		return formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	return nil
}

func (es *V7) CreateIndex(esSetting IESSettings) error {
	indexBodyMap := lo.Assign(
		esSetting.GetSettings(),
		esSetting.GetMappings(),
		esSetting.GetAliases(),
	)

	indexSettingsBytes, _ := json.Marshal(indexBodyMap)

	req := esapi.IndicesCreateRequest{
		Index: esSetting.GetIndex(),
		Body:  bytes.NewBuffer(indexSettingsBytes),
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode == 400 {
		return nil
	}

	if res.IsError() {
		return formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	return nil
}

func (es *V7) IndexExisted(indexName string) (bool, error) {
	res, err := es.Client.Indices.Exists([]string{indexName})
	if err != nil {
		return false, errors.WithStack(err)
	}

	if res.StatusCode == 404 {
		return false, nil
	}

	if res.IsError() {
		return false, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	return res.StatusCode == 200, nil
}

func (es *V7) DeleteIndex(index string) error {
	res, err := es.Client.Indices.Delete([]string{index})
	if err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode == 404 {
		return nil
	}

	if res.IsError() {
		return formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	return nil
}

func (es *V7) GetIndexes() ([]string, error) {
	res, err := es.Client.Cat.Indices()
	if err != nil {
		log.Fatalf("Error getting indices: %s", err)
		return nil, err
	}

	if res.IsError() {
		return nil, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	var indices []string
	scanner := bufio.NewScanner(res.Body)
	for scanner.Scan() {
		value := scanner.Text()
		segments := strings.Fields(value)
		indices = append(indices, segments[2])
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	return indices, nil
}

func (es *V7) Count(ctx context.Context, index string) (uint64, error) {
	res, err := es.Client.Count(es.Client.Count.WithIndex(index))
	if err != nil {
		return 0, errors.WithStack(err)
	}

	if res.IsError() {
		return 0, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	var countResult map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&countResult); err != nil {
		return 0, errors.WithStack(err)
	}

	return cast.ToUint64(countResult["count"]), nil
}

func (es *V7) CreateTemplate(ctx context.Context, name string, body map[string]interface{}) error {
	bodyBytes, _ := json.Marshal(body)
	res, err := es.Client.Indices.PutTemplate(name, bytes.NewReader(bodyBytes))
	if err != nil {
		return errors.WithStack(err)
	}

	if res.IsError() {
		return formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()
	return nil
}

func (es *V7) ClusterHealth(ctx context.Context) (map[string]interface{}, error) {
	// Get Cluster Health
	res, err := es.Client.Cluster.Health()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.IsError() {
		return nil, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	var clusterHealthResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&clusterHealthResp); err != nil {
		return nil, errors.WithStack(err)
	}

	return clusterHealthResp, nil
}

func (es *V7) GetInfo(ctx context.Context) (map[string]interface{}, error) {
	// Get Cluster Health
	res, err := es.Client.Cluster.GetSettings()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.IsError() {
		return nil, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	var clusterHealthResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&clusterHealthResp); err != nil {
		return nil, errors.WithStack(err)
	}

	return clusterHealthResp, nil
}

func (es *V7) GetAddresses() []string {
	return es.Addresses
}

func (es *V7) GetUser() string {
	return es.User
}

func (es *V7) GetPassword() string {
	return es.Password
}
