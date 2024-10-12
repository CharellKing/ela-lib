package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/bytedance/gopkg/collection/skipmap"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	lop "github.com/samber/lo/parallel"
	"github.com/spf13/cast"
	"hash/fnv"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	everyLogTime = 1 * time.Minute
)

const defaultParallelism = 12
const defaultScrollSize = 2000
const defaultScrollTime = 10
const defaultSliceSize = 10
const defaultBufferCount = 10000
const defaultActionSize = 10 // MB
const defaultActionParallelism = 20

type Migrator struct {
	err error

	ctx context.Context

	SourceES es2.ES
	TargetES es2.ES

	IndexPair *config.IndexPair

	ScrollSize uint

	ScrollTime uint

	SliceSize uint

	BufferCount uint

	ActionSize uint

	ActionParallelism uint

	IndexFilePair *config.IndexFilePair

	IndexTemplate *config.IndexTemplate

	FileDir string

	Ids []string
}

func NewMigratorWithConfig(ctx context.Context, srcConfig *config.ESConfig, dstConfig *config.ESConfig) (*Migrator, error) {
	srcES, err := es2.NewESV0(srcConfig).GetES()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	dstES, err := es2.NewESV0(dstConfig).GetES()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return NewMigrator(ctx, srcES, dstES), nil
}

func NewMigrator(ctx context.Context, srcES es2.ES, dstES es2.ES) *Migrator {
	if lo.IsNotEmpty(srcES) {
		ctx = utils.SetCtxKeySourceESVersion(ctx, srcES.GetClusterVersion())
	}

	if lo.IsNotEmpty(dstES) {
		ctx = utils.SetCtxKeyTargetESVersion(ctx, dstES.GetClusterVersion())
	}

	return &Migrator{
		err:               nil,
		ctx:               ctx,
		SourceES:          srcES,
		TargetES:          dstES,
		ScrollSize:        defaultScrollSize,
		ScrollTime:        defaultScrollTime,
		SliceSize:         defaultSliceSize,
		BufferCount:       defaultBufferCount,
		ActionParallelism: defaultActionParallelism,
		ActionSize:        defaultActionSize,
	}
}

func (m *Migrator) GetCtx() context.Context {
	return m.ctx
}

func (m *Migrator) addDateTimeFixFields(ctx context.Context, fieldMap map[string]interface{}) context.Context {
	if !strings.HasPrefix(utils.GetCtxKeySourceESVersion(ctx), "5.") {
		return ctx
	}

	dateTimeFixFields := make(map[string]string)
	for fieldName, fieldAttrs := range fieldMap {
		fieldAttrMap := cast.ToStringMap(fieldAttrs)
		if cast.ToString(fieldAttrMap["type"]) == "date" {
			format := cast.ToString(fieldAttrMap["format"])
			if strings.HasPrefix(format, "yyyy-MM-dd HH:mm:ss:S") {
				dateTimeFixFields[fieldName] = format
			}
		}
	}

	return utils.SetCtxKeyDateTimeFormatFixFields(ctx, dateTimeFixFields)
}

func (m *Migrator) buildIndexPairContext() (context.Context, error) {
	ctx := utils.SetCtxKeySourceObject(m.ctx, m.IndexPair.SourceIndex)
	ctx = utils.SetCtxKeyTargetObject(ctx, m.IndexPair.TargetIndex)

	var err error
	sourceSetting, err := m.SourceES.GetIndexMappingAndSetting(m.IndexPair.SourceIndex)
	if err != nil {
		return ctx, errors.WithStack(err)
	}

	targetSetting, err := m.TargetES.GetIndexMappingAndSetting(m.IndexPair.TargetIndex)
	if err != nil {
		return ctx, errors.WithStack(err)
	}

	ctx = utils.SetCtxKeySourceIndexSetting(ctx, sourceSetting)
	ctx = utils.SetCtxKeyTargetIndexSetting(ctx, targetSetting)
	ctx = utils.SetCtxKeySourceFieldMap(ctx, sourceSetting.GetFieldMap())
	ctx = m.addDateTimeFixFields(ctx, sourceSetting.GetFieldMap())

	if targetSetting != nil {
		ctx = utils.SetCtxKeyTargetFieldMap(ctx, targetSetting.GetFieldMap())
	}

	return ctx, nil
}

func (m *Migrator) WithIndexPair(indexPair config.IndexPair) *Migrator {
	if m.err != nil {
		return m
	}

	return &Migrator{
		err:               m.err,
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		IndexPair:         &indexPair,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionParallelism: m.ActionParallelism,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		IndexFilePair:     m.IndexFilePair,
		IndexTemplate:     m.IndexTemplate,
	}
}

func (m *Migrator) WithIndexTemplate(indexTemplate config.IndexTemplate) *Migrator {
	if m.err != nil {
		return m
	}
	return &Migrator{
		err:               m.err,
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		IndexPair:         m.IndexPair,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionParallelism: m.ActionParallelism,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		IndexFilePair:     m.IndexFilePair,
		IndexTemplate:     &indexTemplate,
	}
}

func (m *Migrator) WithScrollSize(scrollSize uint) *Migrator {
	if m.err != nil {
		return m
	}

	if scrollSize <= 0 {
		scrollSize = defaultScrollSize
	}

	return &Migrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		IndexPair:         m.IndexPair,
		ScrollSize:        scrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionParallelism: m.ActionParallelism,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		IndexFilePair:     m.IndexFilePair,
		IndexTemplate:     m.IndexTemplate,
	}
}

func (m *Migrator) WithScrollTime(scrollTime uint) *Migrator {
	if m.err != nil {
		return m
	}

	if scrollTime <= 0 {
		scrollTime = defaultScrollTime
	}

	return &Migrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		IndexPair:         m.IndexPair,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        scrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionParallelism: m.ActionParallelism,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		IndexFilePair:     m.IndexFilePair,
		IndexTemplate:     m.IndexTemplate,
	}
}

func (m *Migrator) WithSliceSize(sliceSize uint) *Migrator {
	if m.err != nil {
		return m
	}

	if sliceSize <= 0 {
		sliceSize = defaultSliceSize
	}
	return &Migrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		IndexPair:         m.IndexPair,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         sliceSize,
		BufferCount:       m.BufferCount,
		ActionParallelism: m.ActionParallelism,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		IndexFilePair:     m.IndexFilePair,
		IndexTemplate:     m.IndexTemplate,
	}
}

func (m *Migrator) WithBufferCount(sliceSize uint) *Migrator {
	if m.err != nil {
		return m
	}

	if sliceSize <= 0 {
		sliceSize = defaultBufferCount
	}
	return &Migrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		IndexPair:         m.IndexPair,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       sliceSize,
		ActionParallelism: m.ActionParallelism,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		IndexFilePair:     m.IndexFilePair,
		IndexTemplate:     m.IndexTemplate,
	}
}

func (m *Migrator) WithActionParallelism(actionParallelism uint) *Migrator {
	if m.err != nil {
		return m
	}

	if actionParallelism <= 0 {
		actionParallelism = defaultActionParallelism
	}
	return &Migrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		IndexPair:         m.IndexPair,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionParallelism: actionParallelism,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		IndexFilePair:     m.IndexFilePair,
		IndexTemplate:     m.IndexTemplate,
	}
}

func (m *Migrator) WithActionSize(actionSize uint) *Migrator {
	if m.err != nil {
		return m
	}

	if actionSize <= 0 {
		actionSize = defaultActionSize
	}

	return &Migrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		IndexPair:         m.IndexPair,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionParallelism: m.ActionParallelism,
		ActionSize:        actionSize,
		Ids:               m.Ids,
		IndexFilePair:     m.IndexFilePair,
		IndexTemplate:     m.IndexTemplate,
	}
}

func (m *Migrator) WithIds(ids []string) *Migrator {
	if m.err != nil {
		return m
	}

	return &Migrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		IndexPair:         m.IndexPair,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionParallelism: m.ActionParallelism,
		ActionSize:        m.ActionSize,
		Ids:               ids,
		IndexFilePair:     m.IndexFilePair,
		IndexTemplate:     m.IndexTemplate,
	}
}

func (m *Migrator) WithIndexFilePair(indexFilePair *config.IndexFilePair) *Migrator {
	if m.err != nil {
		return m
	}

	return &Migrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		IndexPair:         m.IndexPair,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionParallelism: m.ActionParallelism,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		IndexFilePair:     indexFilePair,
		IndexTemplate:     m.IndexTemplate,
	}
}

func (m *Migrator) CopyIndexSettings(force bool) error {
	if m.err != nil {
		return errors.WithStack(m.err)
	}

	ctx, err := m.buildIndexPairContext()
	if err != nil {
		return errors.WithStack(err)
	}

	if err := m.copyIndexSettings(ctx, m.IndexPair.TargetIndex, force); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (m *Migrator) createTemplate() error {
	indexes, err := m.SourceES.GetIndexes()
	if err != nil {
		return errors.WithStack(err)
	}

	var newPatterns []string
	for _, pattern := range m.IndexTemplate.Patterns {
		newPatterns = append(newPatterns, fmt.Sprintf("^%s$", strings.ReplaceAll(pattern, "*", ".*")))
	}

	var matchedIndex string
	for _, index := range indexes {
		for _, pattern := range newPatterns {
			ok, err := regexp.Match(pattern, []byte(index))
			if err != nil {
				return errors.WithStack(err)
			}

			if ok {
				matchedIndex = index
				break
			}
		}

		if matchedIndex != "" {
			break
		}
	}

	if lo.IsEmpty(matchedIndex) {
		return errors.New("no matched pattern index in source es")
	}

	sourceESSetting, err := m.SourceES.GetIndexMappingAndSetting(matchedIndex)
	if err != nil {
		return errors.WithStack(err)
	}

	templateSetting := m.GetTargetESTemplateSetting(sourceESSetting, m.IndexTemplate.Patterns, m.IndexTemplate.Order)
	if err := m.TargetES.CreateTemplate(m.ctx, m.IndexTemplate.Name, templateSetting); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (m *Migrator) CreateTemplate() error {
	if m.err != nil {
		return errors.WithStack(m.err)
	}

	if err := m.createTemplate(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func getQueryMap(docIds []string) map[string]interface{} {
	if len(docIds) <= 0 {
		return nil
	}
	return map[string]interface{}{
		"query": map[string]interface{}{
			"terms": map[string]interface{}{
				"_id": docIds,
			},
		},
	}
}

func (m *Migrator) SyncDiff() (*DiffResult, error) {
	if m.err != nil {
		return nil, errors.WithStack(m.err)
	}

	ctx, err := m.buildIndexPairContext()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var errs utils.Errs
	diffResult, err := m.compare()
	if err != nil {
		errs.Add(err)
	}

	if len(diffResult.CreateDocs) > 0 {
		utils.GetLogger(ctx).Debugf("sync with create docs: %+v", len(diffResult.CreateDocs))
		if err := m.syncUpsert(ctx, getQueryMap(diffResult.CreateDocs), es2.OperationCreate); err != nil {
			errs.Add(errors.WithStack(err))
		}
	}

	if len(diffResult.UpdateDocs) > 0 {
		utils.GetLogger(ctx).Debugf("sync with update docs: %+v", len(diffResult.UpdateDocs))
		if err := m.syncUpsert(ctx, getQueryMap(diffResult.UpdateDocs), es2.OperationUpdate); err != nil {
			errs.Add(errors.WithStack(err))
		}
	}

	if len(diffResult.DeleteDocs) > 0 {
		utils.GetLogger(ctx).Debugf("sync with delete docs: %+v", len(diffResult.DeleteDocs))
		if err := m.syncUpsert(ctx, getQueryMap(diffResult.DeleteDocs), es2.OperationDelete); err != nil {
			errs.Add(errors.WithStack(err))
		}
	}
	return diffResult, errs.Ret()
}

func (m *Migrator) getESIndexFields(es es2.ES) (map[string]interface{}, error) {
	esSettings, err := es.GetIndexMappingAndSetting(m.IndexPair.SourceIndex)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	propertiesMap := esSettings.GetProperties()

	return cast.ToStringMap(propertiesMap["properties"]), nil
}

func (m *Migrator) getKeywordFields(ctx context.Context) ([]string, error) {
	sourceEsFieldMap := utils.GetCtxKeySourceFieldMap(ctx)

	targetEsFieldMap := utils.GetCtxKeyTargetFieldMap(ctx)

	var keywordFields []string
	for fieldName, fieldAttrs := range sourceEsFieldMap {
		if _, ok := targetEsFieldMap[fieldName]; !ok {
			continue
		}

		fieldAttrMap := cast.ToStringMap(fieldAttrs)
		fieldType := cast.ToString(fieldAttrMap["type"])
		if fieldType == "keyword" {
			keywordFields = append(keywordFields, fieldName)
			continue
		}
	}

	return keywordFields, nil
}

func (m *Migrator) getDocHash(doc *es2.Doc) uint64 {
	h := fnv.New64a()
	jsonData, _ := json.Marshal(doc.Source)
	_, _ = h.Write(jsonData)
	return h.Sum64()
}

func (m *Migrator) handleMultipleErrors(errCh chan error) chan utils.Errs {
	errsCh := make(chan utils.Errs, 1)

	utils.GoRecovery(m.GetCtx(), func() {
		errs := utils.Errs{}
		for {
			err, ok := <-errCh
			if !ok {
				break
			}

			errs.Add(err)
		}
		errsCh <- errs
		close(errsCh)
	})
	return errsCh
}

func (m *Migrator) compare() (*DiffResult, error) {
	ctx, err := m.buildIndexPairContext()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	keywordFields, err := m.getKeywordFields(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	errCh := make(chan error)
	errsCh := m.handleMultipleErrors(errCh)

	queryMap := getQueryMap(m.Ids)

	sourceDocCh, sourceTotal := m.search(ctx, m.SourceES, m.IndexPair.SourceIndex, queryMap, keywordFields, errCh, true)

	targetDocCh, targetTotal := m.search(ctx, m.TargetES, m.IndexPair.TargetIndex, queryMap, keywordFields, errCh, true)

	var (
		sourceCount atomic.Uint64
		targetCount atomic.Uint64
		diffResult  DiffResult
	)

	sourceDocHashMap := skipmap.NewString()
	targetDocHashMap := skipmap.NewString()

	lastPrintTime := time.Now()

	var wg sync.WaitGroup
	for i := uint(0); i < m.ActionParallelism; i++ {
		wg.Add(1)
		utils.GoRecovery(m.GetCtx(), func() {
			defer wg.Done()

			for {
				var (
					sourceResult *es2.Doc
					targetResult *es2.Doc

					sourceOk bool
					targetOk bool
				)

				sourceResult, sourceOk = <-sourceDocCh
				targetResult, targetOk = <-targetDocCh

				if !sourceOk && !targetOk {
					break
				}

				if sourceResult != nil {
					sourceCount.Add(1)
					sourceDocHashMap.Store(sourceResult.ID, sourceResult.Hash)
				}

				if targetResult != nil {
					targetCount.Add(1)
					targetDocHashMap.Store(targetResult.ID, targetResult.Hash)
				}

				if time.Now().Sub(lastPrintTime) > everyLogTime {
					sourceCountValue := sourceCount.Load()
					targetCountValue := targetCount.Load()
					sourceProgress := cast.ToFloat32(sourceCountValue) / cast.ToFloat32(sourceTotal)
					targetProgress := cast.ToFloat32(targetCountValue) / cast.ToFloat32(targetTotal)
					utils.GetLogger(m.GetCtx()).Infof("compare source progress %.4f (%d, %d, %d), "+
						"target progress %.4f (%d, %d, %d)",
						sourceProgress, sourceCountValue, sourceTotal, len(sourceDocCh),
						targetProgress, targetCountValue, targetTotal, len(targetDocCh))
					lastPrintTime = time.Now()
				}

				if sourceResult != nil {
					targetHashValue, ok := targetDocHashMap.Load(sourceResult.ID)
					if ok {
						if sourceResult.Hash != targetHashValue {
							diffResult.addUpdateDoc(sourceResult.ID)
						} else {
							diffResult.SameCount.Add(1)
						}

						targetDocHashMap.Delete(sourceResult.ID)
						sourceDocHashMap.Delete(sourceResult.ID)
					}
				}

				if targetResult != nil {
					sourceHashValue, ok := sourceDocHashMap.Load(targetResult.ID)
					if ok {
						if targetResult.Hash != sourceHashValue {
							diffResult.addUpdateDoc(targetResult.ID)
						} else {
							diffResult.SameCount.Add(1)
						}

						targetDocHashMap.Delete(targetResult.ID)
						sourceDocHashMap.Delete(targetResult.ID)
					}
				}
			}
		})
	}

	wg.Wait()
	close(errCh)

	wg.Add(1)
	utils.GoRecovery(m.ctx, func() {
		defer wg.Done()
		sourceDocHashMap.Range(func(key string, value interface{}) bool {
			diffResult.addCreateDoc(key)
			return true
		})
	})

	wg.Add(1)
	utils.GoRecovery(m.ctx, func() {
		defer wg.Done()
		targetDocHashMap.Range(func(key string, value interface{}) bool {
			diffResult.addDeleteDoc(cast.ToString(key))
			return true
		})
	})

	wg.Wait()

	errs := <-errsCh
	return &diffResult, errors.WithStack(errs.Ret())
}

type DiffResult struct {
	SameCount   atomic.Uint64
	CreateCount atomic.Uint64
	UpdateCount atomic.Uint64
	DeleteCount atomic.Uint64

	CreateDocs []string

	UpdateDocs []string
	updateLock sync.Mutex

	DeleteDocs []string
}

func (diffResult *DiffResult) toStr() string {
	return fmt.Sprintf("same: %d, create: %d, update: %d, delete: %d, total: %d, percent: %0.4f",
		diffResult.SameCount.Load(), diffResult.CreateCount.Load(), diffResult.UpdateCount.Load(),
		diffResult.DeleteCount.Load(), diffResult.Total(), diffResult.Percent())
}
func (diffResult *DiffResult) addUpdateDoc(docId string) {
	diffResult.UpdateCount.Add(1)
	diffResult.updateLock.Lock()
	defer diffResult.updateLock.Unlock()
	diffResult.UpdateDocs = append(diffResult.UpdateDocs, docId)
}

func (diffResult *DiffResult) addCreateDoc(docId string) {
	diffResult.CreateCount.Add(1)
	diffResult.CreateDocs = append(diffResult.CreateDocs, docId)
}

func (diffResult *DiffResult) addDeleteDoc(docId string) {
	diffResult.DeleteCount.Add(1)
	diffResult.DeleteDocs = append(diffResult.DeleteDocs, docId)
}

func (diffResult *DiffResult) HasDiff() bool {
	return diffResult.CreateCount.Load() > 0 || diffResult.UpdateCount.Load() > 0 || diffResult.DeleteCount.Load() > 0
}

func (diffResult *DiffResult) Total() uint64 {
	return diffResult.SameCount.Load() + diffResult.CreateCount.Load() + diffResult.UpdateCount.Load() + diffResult.DeleteCount.Load()
}

func (diffResult *DiffResult) Percent() float64 {
	return float64(diffResult.Total()-diffResult.SameCount.Load()) / float64(diffResult.Total())
}

func (m *Migrator) Compare() (*DiffResult, error) {
	if m.err != nil {
		return nil, errors.WithStack(m.err)
	}

	diffResult, err := m.compare()
	return diffResult, errors.WithStack(err)
}

func (m *Migrator) Sync(force bool) error {
	if m.err != nil {
		return errors.WithStack(m.err)
	}

	ctx, err := m.buildIndexPairContext()
	if err != nil {
		return errors.WithStack(err)
	}

	utils.GetLogger(m.ctx).Debugf("sync with force: %+v", force)
	if err := m.copyIndexSettings(ctx, m.IndexPair.TargetIndex, force); err != nil {
		utils.GetLogger(m.GetCtx()).Errorf("copy index settings %+v", err)
	}
	if err := m.syncUpsert(ctx, getQueryMap(m.Ids), es2.OperationCreate); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *Migrator) searchSingleSlice(ctx context.Context, wg *sync.WaitGroup, es es2.ES,
	index string, query map[string]interface{}, sortFields []string,
	sliceId *uint, sliceSize *uint, docCh chan *es2.Doc, errCh chan error, needHash bool) {

	utils.GoRecovery(m.GetCtx(), func() {
		var (
			scrollResult *es2.ScrollResult
			err          error
		)
		defer func() {
			wg.Done()
			if scrollResult != nil {
				if err := es.ClearScroll(scrollResult.ScrollId); err != nil {
					utils.GetLogger(m.GetCtx()).Errorf("clear scroll %+v", err)
				}
			}
		}()

		func() {
			scrollResult, err = es.NewScroll(ctx, index, &es2.ScrollOption{
				Query:      query,
				SortFields: sortFields,
				ScrollSize: m.ScrollSize,
				ScrollTime: m.ScrollTime,
				SliceId:    sliceId,
				SliceSize:  sliceSize,
			})

			if err != nil {
				utils.GetLogger(m.GetCtx()).Errorf("searchSingleSlice error: %+v", err)
				errCh <- errors.WithStack(err)
			}

			if scrollResult == nil {
				return
			}

		}()

		for {
			if scrollResult == nil || len(scrollResult.Docs) <= 0 {
				utils.GetLogger(m.GetCtx()).Infof("scroll slice %d exit", lo.Ternary(sliceId != nil, *sliceId, 0))
				break
			}

			scrollResult.Docs = lop.Map(scrollResult.Docs, func(doc *es2.Doc, _ int) *es2.Doc {
				var fixErr error
				doc, fixErr = es2.FixDoc(ctx, doc)
				if fixErr != nil {
					errCh <- fixErr
				}
				if needHash {
					doc.Hash = m.getDocHash(doc)
				}
				return doc
			})

			for _, doc := range scrollResult.Docs {
				docCh <- doc
			}

			if scrollResult, err = es.NextScroll(ctx, scrollResult.ScrollId, m.ScrollTime); err != nil {
				utils.GetLogger(m.GetCtx()).Errorf("searchSingleSlice error: %+v", err)
				errCh <- errors.WithStack(err)
			}
		}
	})
}

func (m *Migrator) search(ctx context.Context, es es2.ES, index string, query map[string]interface{},
	sortFields []string, errCh chan error, needHash bool) (chan *es2.Doc, uint64) {
	docCh := make(chan *es2.Doc, m.BufferCount)
	var wg sync.WaitGroup

	total, err := es.Count(m.GetCtx(), index)
	if err != nil {
		errCh <- errors.WithStack(err)
		close(errCh)
		close(docCh)
		return nil, 0
	}

	if m.SliceSize <= 1 {
		wg.Add(1)
		m.searchSingleSlice(ctx, &wg, es, index, query, sortFields, nil, nil, docCh, errCh, needHash)
	} else {
		for i := uint(0); i < m.SliceSize; i++ {
			idx := i
			wg.Add(1)
			m.searchSingleSlice(ctx, &wg, es, index, query, sortFields, &idx, &m.SliceSize, docCh, errCh, needHash)
		}
	}
	utils.GoRecovery(m.GetCtx(), func() {
		wg.Wait()
		close(docCh)
	})

	return docCh, total
}

func (m *Migrator) singleBulkWorker(docCh <-chan *es2.Doc, index string, total uint64, count *atomic.Uint64,
	operation es2.Operation, errCh chan error) {
	var buf bytes.Buffer

	lastPrintTime := time.Now()
	for {
		v, ok := <-docCh
		if !ok {
			break
		}
		v.Op = operation
		count.Add(1)
		percent := cast.ToFloat32(count.Load()) / cast.ToFloat32(total)

		if time.Now().Sub(lastPrintTime) > everyLogTime {
			utils.GetLogger(m.GetCtx()).Infof("bulk progress %.4f (%d, %d, %d)",
				percent, count.Load(), total, len(docCh))
			lastPrintTime = time.Now()
		}
		switch operation {
		case es2.OperationCreate:
			if err := m.TargetES.BulkBody(index, &buf, v); err != nil {
				errCh <- errors.WithStack(err)
			}
		case es2.OperationUpdate:
			if err := m.TargetES.BulkBody(index, &buf, v); err != nil {
				errCh <- errors.WithStack(err)
			}
		case es2.OperationDelete:
			if err := m.TargetES.BulkBody(index, &buf, v); err != nil {
				errCh <- errors.WithStack(err)
			}
		default:
			utils.GetLogger(m.ctx).Error("unknown operation")
		}

		if buf.Len() >= cast.ToInt(m.ActionSize)*1024*1024 {
			if err := m.TargetES.Bulk(&buf); err != nil {
				errCh <- errors.WithStack(err)
			}
			buf.Reset()
		}
	}

	if buf.Len() > 0 {
		if err := m.TargetES.Bulk(&buf); err != nil {
			errCh <- errors.WithStack(err)
		}
		buf.Reset()
	}
}

func (m *Migrator) getOperationTitle(operation es2.Operation) string {
	switch operation {
	case es2.OperationCreate:
		return "create"
	case es2.OperationUpdate:
		return "update"
	case es2.OperationDelete:
		return "delete"
	default:
		return ""
	}
}
func (m *Migrator) bulkWorker(docCh <-chan *es2.Doc, index string, total uint64, operation es2.Operation, errCh chan error) {
	var wg sync.WaitGroup
	var count atomic.Uint64

	if m.ActionParallelism <= 1 {
		m.singleBulkWorker(docCh, index, total, &count, operation, errCh)
	}

	wg.Add(cast.ToInt(m.ActionParallelism))
	for i := 0; i < cast.ToInt(m.ActionParallelism); i++ {
		utils.GoRecovery(m.ctx, func() {
			defer wg.Done()
			m.singleBulkWorker(docCh, index, total, &count, operation, errCh)
		})
	}

	wg.Wait()

	percent := cast.ToFloat32(count.Load()) / cast.ToFloat32(total)
	utils.GetLogger(m.GetCtx()).Infof("bulk progress %.4f (%d, %d, %d)",
		percent, count.Load(), total, len(docCh))
}

func (m *Migrator) singleBulkFileWorker(doc <-chan *es2.Doc, total uint64, count *atomic.Uint64,
	filepath string, errCh chan error) {
	f, err := os.Create(filepath)
	if err != nil {
		errCh <- errors.WithStack(err)
		return
	}
	defer func() {
		_ = f.Close()
	}()

	var buf strings.Builder
	writer := bufio.NewWriter(f)

	lastPrintTime := time.Now()
	for {
		v, ok := <-doc
		if !ok {
			break
		}
		count.Add(1)
		percent := cast.ToFloat32(count.Load()) / cast.ToFloat32(total)

		if time.Now().Sub(lastPrintTime) > everyLogTime {
			utils.GetLogger(m.GetCtx()).Infof("bulk progress %.4f (%d, %d, %d)",
				percent, count.Load(), total, len(doc))
			lastPrintTime = time.Now()
		}

		buf.Write(v.DumpFileBytes())
		buf.WriteString("\n")

		if buf.Len() >= cast.ToInt(m.ActionSize)*1024*1024 {
			if _, err := writer.WriteString(buf.String()); err != nil {
				errCh <- errors.WithStack(err)
			}
			_ = writer.Flush()
			buf.Reset()
		}
	}

	if buf.Len() > 0 {
		if _, err := f.WriteString(buf.String()); err != nil {
			errCh <- errors.WithStack(err)
		}
		_ = writer.Flush()
		buf.Reset()
	}

}

func (m *Migrator) bulkFileWorker(doc <-chan *es2.Doc, total uint64, files []string, errCh chan error) {
	var wg sync.WaitGroup
	var count atomic.Uint64

	wg.Add(len(files))
	for _, file := range files {
		utils.GoRecovery(m.ctx, func() {
			defer wg.Done()
			m.singleBulkFileWorker(doc, total, &count, file, errCh)
		})
	}
	wg.Wait()

	percent := cast.ToFloat32(count.Load()) / cast.ToFloat32(total)
	utils.GetLogger(m.GetCtx()).Infof("bulk progress %.4f (%d, %d, %d)",
		percent, count.Load(), total, len(doc))
}

func (m *Migrator) syncUpsert(ctx context.Context, query map[string]interface{}, operation es2.Operation) error {
	errCh := make(chan error)
	errsCh := m.handleMultipleErrors(errCh)

	var (
		docCh chan *es2.Doc
		total uint64
	)
	if operation == es2.OperationDelete {
		docCh, total = m.search(ctx, m.TargetES, m.IndexPair.SourceIndex, query, nil, errCh, false)
	} else {
		docCh, total = m.search(ctx, m.SourceES, m.IndexPair.SourceIndex, query, nil, errCh, false)
	}
	m.bulkWorker(docCh, m.IndexPair.TargetIndex, total, operation, errCh)
	close(errCh)
	errs := <-errsCh
	return errs.Ret()
}

func (m *Migrator) copyIndexSettings(ctx context.Context, targetIndex string, force bool) error {
	existed, err := m.TargetES.IndexExisted(targetIndex)
	if err != nil {
		return errors.WithStack(err)
	}

	if existed && !force {
		return nil
	}

	if existed {
		if err := m.TargetES.DeleteIndex(targetIndex); err != nil {
			return errors.WithStack(err)
		}
	}

	sourceESSetting := utils.GetCtxKeySourceIndexSetting(ctx).(es2.IESSettings)

	targetESSetting := m.GetTargetESSetting(sourceESSetting, targetIndex)

	if err := m.TargetES.CreateIndex(targetESSetting); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (m *Migrator) GetTargetESSetting(sourceESSetting es2.IESSettings, targetIndex string) es2.IESSettings {
	if strings.HasPrefix(m.TargetES.GetClusterVersion(), "8.") {
		return sourceESSetting.ToTargetV8Settings(targetIndex)
	} else if strings.HasPrefix(m.TargetES.GetClusterVersion(), "7.") {
		return sourceESSetting.ToTargetV7Settings(targetIndex)
	} else if strings.HasPrefix(m.TargetES.GetClusterVersion(), "6.") {
		return sourceESSetting.ToTargetV6Settings(targetIndex)
	} else if strings.HasPrefix(m.TargetES.GetClusterVersion(), "5.") {
		return sourceESSetting.ToTargetV5Settings(targetIndex)
	}

	return nil
}

func (m *Migrator) GetTargetESTemplateSetting(sourceESSetting es2.IESSettings, patterns []string, order int) map[string]interface{} {
	if strings.HasPrefix(m.TargetES.GetClusterVersion(), "8.") {
		return sourceESSetting.ToV8TemplateSettings(patterns, order)
	} else if strings.HasPrefix(m.TargetES.GetClusterVersion(), "7.") {
		return sourceESSetting.ToV7TemplateSettings(patterns, order)
	} else if strings.HasPrefix(m.TargetES.GetClusterVersion(), "6.") {
		return sourceESSetting.ToV6TemplateSettings(patterns, order)
	} else if strings.HasPrefix(m.TargetES.GetClusterVersion(), "5.") {
		return sourceESSetting.ToV5TemplateSettings(patterns, order)
	}

	return nil
}

func (m *Migrator) getTotalFromFile(filePath string) (uint64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	defer func() {
		_ = file.Close()
	}()

	scanner := bufio.NewScanner(file)
	var firstLineText string
	if scanner.Scan() {
		firstLineText = scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		return 0, errors.WithStack(err)
	}

	total, err := cast.ToUint64E(firstLineText)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return total, nil
}

func (m *Migrator) convertLineToDoc(ctx context.Context, line []byte) (*es2.Doc, error) {
	var (
		doc es2.Doc
		err error
	)

	line = bytes.TrimSpace(line)
	if len(line) <= 0 {
		return nil, nil
	}

	if err = json.Unmarshal(line, &doc); err != nil {
		return nil, errors.WithStack(err)
	}
	newDoc, err := es2.FixDoc(ctx, &doc)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return newDoc, nil

}
func (m *Migrator) getFileContentWithSlice(ctx context.Context, wg *sync.WaitGroup, indexFileSetting *IndexFileSetting, sliceId *uint, sliceSize *uint, docCh chan *es2.Doc, errCh chan error) {
	utils.GoRecovery(m.GetCtx(), func() {
		defer wg.Done()
		for _, filePath := range indexFileSetting.Files {
			func() {
				file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
				if err != nil {
					errCh <- errors.WithStack(err)
					return
				}

				defer func() {
					_ = file.Close()
				}()

				var lineIdx int64 = 0

				var interval int64 = 1
				if sliceSize != nil && *sliceSize > 1 {
					interval = int64(*sliceSize)
				}

				scanner := bufio.NewScanner(file)

				var lines []string
				for scanner.Scan() {
					line := scanner.Text()
					if lineIdx%interval == int64(*sliceId) {
						lines = append(lines, line)
					}

					if cast.ToUint(len(lines)) >= m.ScrollSize {
						for _, doc := range lop.Map(lines, func(line string, _ int) *es2.Doc {
							doc, err := m.convertLineToDoc(ctx, []byte(line))
							if err != nil {
								errCh <- errors.WithStack(err)
							}
							doc, err = es2.FixDoc(ctx, doc)
							if err != nil {
								errCh <- errors.WithStack(err)
							}
							return doc
						}) {
							if doc != nil {
								docCh <- doc
							}
						}
						lines = []string{}
					}

					lineIdx++
				}

				if len(lines) > 0 {
					for _, doc := range lop.Map(lines, func(line string, _ int) *es2.Doc {
						doc, err := m.convertLineToDoc(ctx, []byte(line))
						if err != nil {
							errCh <- errors.WithStack(err)
						}
						doc, err = es2.FixDoc(ctx, doc)
						if err != nil {
							errCh <- errors.WithStack(err)
						}
						return doc
					}) {
						if doc != nil {
							docCh <- doc
						}
					}
					lines = []string{}
				}
			}()
		}
	})
}

func (m *Migrator) scrollFile(ctx context.Context, indexFileSetting *IndexFileSetting, errCh chan error) chan *es2.Doc {
	docCh := make(chan *es2.Doc, m.BufferCount)
	var wg sync.WaitGroup

	if len(indexFileSetting.Files) > 0 {
		for i := uint(0); i < m.SliceSize; i++ {
			idx := i
			wg.Add(1)
			m.getFileContentWithSlice(ctx, &wg, indexFileSetting, &idx, &m.SliceSize, docCh, errCh)
		}
	}

	utils.GoRecovery(ctx, func() {
		wg.Wait()
		close(docCh)
	})

	return docCh
}

func (m *Migrator) buildImportIndexFilePairContext() (context.Context, *IndexFileSetting, error) {
	ctx := m.ctx
	settingFile := fmt.Sprintf("%s/setting.json", m.IndexFilePair.IndexFileDir)
	settingBytes, err := os.ReadFile(settingFile)
	if err != nil {
		return ctx, nil, errors.WithStack(err)
	}
	settingMap := make(map[string]interface{})
	if err := json.Unmarshal(settingBytes, &settingMap); err != nil {
		return ctx, nil, errors.WithStack(err)
	}

	indexFileSetting, err := LoadIndexFileSettingFromMap(settingMap)
	if err != nil {
		return ctx, nil, errors.WithStack(err)
	}

	ctx = utils.SetCtxKeySourceObject(m.ctx, m.IndexFilePair.IndexFileDir)
	ctx = utils.SetCtxKeyTargetObject(ctx, m.IndexFilePair.Index)

	ctx = utils.SetCtxKeySourceESVersion(ctx, indexFileSetting.ESVersion)
	ctx = utils.SetCtxKeySourceIndexSetting(ctx, indexFileSetting.Settings)
	ctx = utils.SetCtxKeySourceFieldMap(ctx, indexFileSetting.Settings.GetFieldMap())
	ctx = m.addDateTimeFixFields(ctx, indexFileSetting.Settings.GetFieldMap())

	//targetSetting, err := m.TargetES.GetIndexMappingAndSetting(m.IndexFilePair.Index)
	//if err != nil {
	//	return ctx, nil, errors.WithStack(err)
	//}
	//
	//ctx = utils.SetCtxKeyTargetIndexSetting(ctx, targetSetting)
	//ctx = utils.SetCtxKeyTargetFieldMap(ctx, targetSetting.GetFieldMap())
	//
	//ctx = m.addDateTimeFixFields(ctx, targetSetting.GetFieldMap())

	return ctx, indexFileSetting, nil
}

func (m *Migrator) buildExportIndexFilePairContext() (context.Context, error) {
	ctx := m.ctx
	ctx = utils.SetCtxKeySourceObject(m.ctx, m.IndexFilePair.Index)
	ctx = utils.SetCtxKeyTargetObject(ctx, m.IndexFilePair.IndexFileDir)

	var err error
	sourceSetting, err := m.SourceES.GetIndexMappingAndSetting(m.IndexFilePair.Index)
	if err != nil {
		return ctx, errors.WithStack(err)
	}

	ctx = utils.SetCtxKeySourceIndexSetting(ctx, sourceSetting)
	ctx = utils.SetCtxKeySourceFieldMap(ctx, sourceSetting.GetFieldMap())

	ctx = m.addDateTimeFixFields(ctx, sourceSetting.GetFieldMap())

	return ctx, nil
}

func (m *Migrator) Import(force bool) error {
	if m.err != nil {
		return errors.WithStack(m.err)
	}

	ctx, indexFileSetting, err := m.buildImportIndexFilePairContext()
	if err != nil {
		return errors.WithStack(err)
	}

	if err := m.copyIndexSettings(ctx, m.IndexFilePair.Index, force); err != nil {
		utils.GetLogger(m.GetCtx()).Errorf("copy index settings %+v", err)
	}

	errCh := make(chan error)
	errsCh := m.handleMultipleErrors(errCh)

	var (
		docCh chan *es2.Doc
	)

	docCh = m.scrollFile(ctx, indexFileSetting, errCh)
	m.bulkWorker(docCh, m.IndexFilePair.Index, indexFileSetting.Total, es2.OperationCreate, errCh)
	close(errCh)
	errs := <-errsCh
	return errs.Ret()
}

type IndexFileSetting struct {
	Total     uint64          `json:"total"`
	Settings  es2.IESSettings `json:"settings"`
	Files     []string        `json:"files"`
	ESVersion string          `json:"es_version"`
	Index     string          `json:"index"`
}

func LoadIndexFileSettingFromMap(settingMap map[string]interface{}) (*IndexFileSetting, error) {
	setting := &IndexFileSetting{}
	setting.Total = cast.ToUint64(settingMap["total"])
	setting.ESVersion = cast.ToString(settingMap["es_version"])
	setting.Index = cast.ToString(settingMap["index"])
	setting.Files = cast.ToStringSlice(settingMap["files"])

	var err error
	setting.Settings, err = es2.GetESSettings(setting.ESVersion, cast.ToStringMap(settingMap["settings"]))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return setting, nil
}

func (m *Migrator) saveIndexFileSetting(ctx context.Context) (*IndexFileSetting, error) {
	total, err := m.SourceES.Count(ctx, m.IndexFilePair.Index)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	settings := utils.GetCtxKeySourceIndexSetting(ctx).(es2.IESSettings)
	indexFileSetting := &IndexFileSetting{
		Total:     total,
		Settings:  settings,
		ESVersion: utils.GetCtxKeySourceESVersion(ctx),
		Index:     m.IndexFilePair.Index,
	}

	var i uint = 0
	for i < m.ActionParallelism {
		filePath := fmt.Sprintf("%s/part-%d", m.IndexFilePair.IndexFileDir, i)
		indexFileSetting.Files = append(indexFileSetting.Files, filePath)
		i++
	}

	_, err = os.Stat(m.IndexFilePair.IndexFileDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.WithStack(err)
	}

	if os.IsNotExist(err) {
		if err := os.MkdirAll(m.IndexFilePair.IndexFileDir, 0755); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	indexFileSettingBytes, _ := json.Marshal(indexFileSetting)
	settingFilePath := fmt.Sprintf("%s/setting.json", m.IndexFilePair.IndexFileDir)
	err = os.WriteFile(settingFilePath, indexFileSettingBytes, 0644)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return indexFileSetting, nil
}

func (m *Migrator) export(ctx context.Context) error {
	indexFileSetting, err := m.saveIndexFileSetting(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	errCh := make(chan error)
	errsCh := m.handleMultipleErrors(errCh)

	var (
		docCh chan *es2.Doc
		total uint64
	)

	query := getQueryMap(m.Ids)
	docCh, total = m.search(ctx, m.SourceES, m.IndexFilePair.Index, query, nil, errCh, false)

	m.bulkFileWorker(docCh, total, indexFileSetting.Files, errCh)
	close(errCh)
	errs := <-errsCh
	return errs.Ret()
}

func (m *Migrator) Export() error {
	if m.err != nil {
		return errors.WithStack(m.err)
	}

	ctx, err := m.buildExportIndexFilePairContext()
	if err != nil {
		return errors.WithStack(err)
	}

	return m.export(ctx)
}
