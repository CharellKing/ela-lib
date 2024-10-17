package es

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/mitchellh/mapstructure"
	lop "github.com/samber/lo/parallel"
	"github.com/spf13/cast"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	elasticsearch5 "github.com/elastic/go-elasticsearch/v5"
	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

type V5 struct {
	*elasticsearch5.Client
	ClusterVersion string
	Settings       IESSettings
	Addresses      []string
	User           string
	Password       string
}

func NewESV5(esConfig *config.ESConfig, clusterVersion string) (*V5, error) {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	client, err := elasticsearch5.NewClient(elasticsearch5.Config{
		Addresses: esConfig.Addresses,
		Username:  esConfig.User,
		Password:  esConfig.Password,
		Transport: transport,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &V5{
		Client:         client,
		ClusterVersion: clusterVersion,
		Addresses:      esConfig.Addresses,
		User:           esConfig.User,
		Password:       esConfig.Password,
	}, nil
}

func (es *V5) GetClusterVersion() string {
	return es.ClusterVersion
}

type ScrollResultV5 struct {
	Took     int    `json:"took,omitempty"`
	ScrollId string `json:"_scroll_id,omitempty"`
	TimedOut bool   `json:"timed_out,omitempty"`
	Hits     struct {
		MaxScore float32       `json:"max_score,omitempty"`
		Total    int           `json:"total,omitempty"`
		Docs     []interface{} `json:"hits,omitempty"`
	} `json:"hits"`
	Shards struct {
		Total      int `json:"total,omitempty"`
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

func (es *V5) fixDatetimeFormatDate(ctx context.Context, doc *Doc) *Doc {
	datetimeFields := utils.GetCtxKeyDateTimeFormatFixFields(ctx)

	for fieldName, fieldValue := range doc.Source {
		if datetimeFieldFormat, ok := datetimeFields[fieldName]; ok {
			fieldValueStr := cast.ToString(fieldValue)
			valueSections := strings.Split(fieldValueStr, ":")
			formatSections := strings.Split(datetimeFieldFormat, ":")
			if len(valueSections) == 3 {
				format := fmt.Sprintf("%%0%dd", len(formatSections[3]))
				valueSections = append(valueSections, fmt.Sprintf(format, 0))
				doc.Source[fieldName] = strings.Join(valueSections, ":")
			} else if len(valueSections) > 3 {
				format := fmt.Sprintf("%%0%dd", len(formatSections[3]))
				secondFraction := cast.ToInt(valueSections[3])
				valueSections[3] = fmt.Sprintf(format, secondFraction)
				doc.Source[fieldName] = strings.Join(valueSections, ":")
			}
		}
	}
	return doc
}

func (es *V5) NewScroll(ctx context.Context, index string, option *ScrollOption) (*ScrollResult, error) {
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
			"field": "_uid",
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

	if res.StatusCode != http.StatusOK {
		return nil, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	var scrollResult ScrollResultV5
	if err := json.NewDecoder(res.Body).Decode(&scrollResult); err != nil {
		return nil, errors.WithStack(err)
	}

	hitDocs := lop.Map(scrollResult.Hits.Docs, func(hit interface{}, _ int) *Doc {
		var hitDoc Doc
		_ = mapstructure.Decode(hit, &hitDoc)
		return &hitDoc
	})

	return &ScrollResult{
		Total:    uint64(scrollResult.Hits.Total),
		Docs:     hitDocs,
		ScrollId: scrollResult.ScrollId,
	}, nil
}

func (es *V5) NextScroll(ctx context.Context, scrollId string, scrollTime uint) (*ScrollResult, error) {
	res, err := es.Client.Scroll(es.Client.Scroll.WithScrollID(scrollId), es.Client.Scroll.WithScroll(time.Duration(scrollTime)*time.Minute))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.StatusCode != http.StatusOK {
		return nil, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	var scrollResult ScrollResultV5
	if err := json.NewDecoder(res.Body).Decode(&scrollResult); err != nil {
		return nil, errors.WithStack(err)
	}

	hitDocs := lop.Map(scrollResult.Hits.Docs, func(hit interface{}, _ int) *Doc {
		var hitDoc Doc
		_ = mapstructure.Decode(hit, &hitDoc)
		return &hitDoc
	})

	return &ScrollResult{
		Total:    uint64(scrollResult.Hits.Total),
		Docs:     hitDocs,
		ScrollId: scrollResult.ScrollId,
	}, nil
}

func (es *V5) ClearScroll(scrollId string) error {
	res, err := es.Client.ClearScroll(es.Client.ClearScroll.WithScrollID(scrollId))
	if err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode != http.StatusOK {
		return formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	return nil
}

func (es *V5) GetIndexMappingAndSetting(index string) (IESSettings, error) {
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

	alias, err := es.GetIndexAliases(index)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return NewV5Settings(setting, mapping, alias, index), nil
}

func (es *V5) GetIndexAliases(index string) (map[string]interface{}, error) {
	// Get alias configuration
	res, err := es.Client.Indices.GetAlias(es.Client.Indices.GetAlias.WithIndex(index))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.StatusCode != http.StatusOK {
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

func (es *V5) GetIndexMapping(index string) (map[string]interface{}, error) {
	// Get settings
	res, err := es.Client.Indices.GetMapping(es.Client.Indices.GetMapping.WithIndex(index))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.StatusCode != http.StatusOK {
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

func (es *V5) GetIndexSettings(index string) (map[string]interface{}, error) {
	// Get settings
	res, err := es.Client.Indices.GetSettings(es.Client.Indices.GetSettings.WithIndex(index))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.StatusCode != http.StatusOK {
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

func (es *V5) CreateIndex(esSetting IESSettings) error {
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

	if res.StatusCode != http.StatusOK {
		return formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	return nil
}

func (es *V5) IndexExisted(indexName string) (bool, error) {
	res, err := es.Client.Indices.Exists([]string{indexName})
	if err != nil {
		return false, errors.WithStack(err)
	}

	if res.StatusCode == 404 {
		return false, nil
	}

	if res.StatusCode != http.StatusOK {
		return false, formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	return res.StatusCode == 200, nil
}

func (es *V5) DeleteIndex(index string) error {
	res, err := es.Client.Indices.Delete([]string{index})
	if err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode != http.StatusOK {
		return formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()
	return nil
}

func (es *V5) BulkBody(index string, buf *bytes.Buffer, doc *Doc) error {
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
			"_type":  doc.Type,
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

func (es *V5) Count(ctx context.Context, index string) (uint64, error) {
	res, err := es.Client.Count(es.Client.Count.WithIndex(index))
	if err != nil {
		return 0, errors.WithStack(err)
	}

	if res.StatusCode != http.StatusOK {
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

func (es *V5) Bulk(buf *bytes.Buffer) error {
	// Execute the bulk request
	res, err := es.Client.Bulk(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode != http.StatusOK {
		return formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	return nil
}

func (es *V5) GetIndexes() ([]string, error) {
	res, err := es.Client.Cat.Indices()
	if err != nil {
		log.Fatalf("Error getting indices: %s", err)
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
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

func (es *V5) CreateTemplate(ctx context.Context, name string, body map[string]interface{}) error {
	bodyBytes, _ := json.Marshal(body)
	res, err := es.Client.Indices.PutTemplate(name, bytes.NewReader(bodyBytes))
	if err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode != http.StatusOK {
		return formatError(res)
	}

	defer func() {
		_ = res.Body.Close()
	}()
	return nil
}

type ClusterHealthRespV5 struct {
	ActivePrimaryShards         int     `json:"active_primary_shards"`
	ActiveShards                int     `json:"active_shards"`
	ActiveShardsPercentAsNumber float64 `json:"active_shards_percent_as_number"`
	ClusterName                 string  `json:"cluster_name"`
	DelayedUnassignedShards     int     `json:"delayed_unassigned_shards"`
	InitializingShards          int     `json:"initializing_shards"`
	NumberOfDataNodes           int     `json:"number_of_data_nodes"`
	NumberOfInFlightFetch       int     `json:"number_of_in_flight_fetch"`
	NumberOfNodes               int     `json:"number_of_nodes"`
	NumberOfPendingTasks        int     `json:"number_of_pending_tasks"`
	RelocatingShards            int     `json:"relocating_shards"`
	Status                      string  `json:"status"`
	TaskMaxWaitingInQueueMillis int     `json:"task_max_waiting_in_queue_millis"`
	TimedOut                    bool    `json:"timed_out"`
	UnassignedShards            int     `json:"unassigned_shards"`
}

func (es *V5) ClusterHealth(ctx context.Context) (map[string]interface{}, error) {
	// Get Cluster Health
	res, err := es.Client.Cluster.Health()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.StatusCode != http.StatusOK {
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

func (es *V5) GetInfo(ctx context.Context) (map[string]interface{}, error) {
	// Get Cluster Health
	res, err := es.Client.Info()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.StatusCode != http.StatusOK {
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

func (es *V5) GetAddresses() []string {
	return es.Addresses
}

func (es *V5) GetUser() string {
	return es.User
}

func (es *V5) GetPassword() string {
	return es.Password
}
