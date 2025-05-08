package task

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

	Query string

	sourceIndexPairProgress *utils.Progress
	targetIndexPairProgress *utils.Progress

	sourceQueueExtrusion *utils.Progress
	targetQueueExtrusion *utils.Progress

	pairProgress *utils.Progress

	isCancelled *bool
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

	return NewMigrator(ctx, srcES, dstES, lo.ToPtr(false)), nil
}

func NewMigrator(ctx context.Context, srcES es2.ES, dstES es2.ES, isCancelled *bool) *Migrator {
	if lo.IsNotEmpty(srcES) {
		ctx = utils.SetCtxKeySourceESVersion(ctx, srcES.GetClusterVersion())
	}

	if lo.IsNotEmpty(dstES) {
		ctx = utils.SetCtxKeyTargetESVersion(ctx, dstES.GetClusterVersion())
	}

	sourceIndexPairProgress := utils.NewProgress("", 0)

	ctx = utils.SetCtxKeySourceIndexPairProgress(ctx, sourceIndexPairProgress)

	targetIndexPairProgress := utils.NewProgress("", 0)
	ctx = utils.SetCtxKeyTargetIndexPairProgress(ctx, targetIndexPairProgress)

	sourceQueueExtrusion := utils.NewExtrusion("", defaultBufferCount)

	ctx = utils.SetCtxKeySourceQueueExtrusion(ctx, sourceQueueExtrusion)

	targetQueueExtrusion := utils.NewExtrusion("", defaultBufferCount)
	ctx = utils.SetCtxKeyTargetQueueExtrusion(ctx, targetQueueExtrusion)

	pairProgress := utils.NewProgress(string(utils.CtxKeyPairProgress), 0)
	ctx = utils.SetCtxKeyPairProgress(ctx, pairProgress)

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

		sourceIndexPairProgress: sourceIndexPairProgress,
		targetIndexPairProgress: targetIndexPairProgress,
		pairProgress:            pairProgress,

		sourceQueueExtrusion: sourceQueueExtrusion,
		targetQueueExtrusion: targetQueueExtrusion,

		isCancelled: isCancelled,
	}
}

func (m *Migrator) GetCtx() context.Context {
	return m.ctx
}

func (m *Migrator) addDateTimeFixFields(ctx context.Context, fieldMap map[string]interface{}) context.Context {
	if !es2.ClusterVersionLte5(utils.GetCtxKeySourceESVersion(ctx)) {
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

func (m *Migrator) buildIndexPairContext() error {
	m.ctx = utils.SetCtxKeySourceObject(m.ctx, m.IndexPair.SourceIndex)
	m.ctx = utils.SetCtxKeyTargetObject(m.ctx, m.IndexPair.TargetIndex)

	var err error
	sourceSetting, err := m.SourceES.GetIndexMappingAndSetting(m.IndexPair.SourceIndex)
	if err != nil {
		return errors.WithStack(err)
	}

	if sourceSetting == nil {
		return errors.Errorf("source index %s not exists", m.IndexPair.SourceIndex)
	}

	targetSetting, err := m.TargetES.GetIndexMappingAndSetting(m.IndexPair.TargetIndex)
	if err != nil {
		return errors.WithStack(err)
	}

	m.ctx = utils.SetCtxKeySourceIndexSetting(m.ctx, sourceSetting)
	m.ctx = utils.SetCtxKeyTargetIndexSetting(m.ctx, targetSetting)
	m.ctx = utils.SetCtxKeySourceFieldMap(m.ctx, sourceSetting.GetFieldMap())
	m.ctx = m.addDateTimeFixFields(m.ctx, sourceSetting.GetFieldMap())

	if targetSetting != nil {
		m.ctx = utils.SetCtxKeyTargetFieldMap(m.ctx, targetSetting.GetFieldMap())
	}

	return nil
}

func (m *Migrator) WithIndexPair(indexPair config.IndexPair) *Migrator {
	if m.err != nil {
		return m
	}

	return &Migrator{
		err:                     m.err,
		ctx:                     m.ctx,
		SourceES:                m.SourceES,
		TargetES:                m.TargetES,
		IndexPair:               &indexPair,
		ScrollSize:              m.ScrollSize,
		ScrollTime:              m.ScrollTime,
		SliceSize:               m.SliceSize,
		BufferCount:             m.BufferCount,
		ActionParallelism:       m.ActionParallelism,
		ActionSize:              m.ActionSize,
		Ids:                     m.Ids,
		IndexFilePair:           m.IndexFilePair,
		IndexTemplate:           m.IndexTemplate,
		Query:                   m.Query,
		sourceIndexPairProgress: m.sourceIndexPairProgress,
		targetIndexPairProgress: m.targetIndexPairProgress,
		pairProgress:            m.pairProgress,

		sourceQueueExtrusion: m.sourceQueueExtrusion,
		targetQueueExtrusion: m.targetQueueExtrusion,

		isCancelled: m.isCancelled,
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
		Query:             m.Query,

		sourceIndexPairProgress: m.sourceIndexPairProgress,
		targetIndexPairProgress: m.targetIndexPairProgress,
		pairProgress:            m.pairProgress,

		sourceQueueExtrusion: m.sourceQueueExtrusion,
		targetQueueExtrusion: m.targetQueueExtrusion,

		isCancelled: m.isCancelled,
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
		Query:             m.Query,

		sourceIndexPairProgress: m.sourceIndexPairProgress,
		targetIndexPairProgress: m.targetIndexPairProgress,
		pairProgress:            m.pairProgress,

		sourceQueueExtrusion: m.sourceQueueExtrusion,
		targetQueueExtrusion: m.targetQueueExtrusion,

		isCancelled: m.isCancelled,
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
		Query:             m.Query,

		sourceIndexPairProgress: m.sourceIndexPairProgress,
		targetIndexPairProgress: m.targetIndexPairProgress,
		pairProgress:            m.pairProgress,

		sourceQueueExtrusion: m.sourceQueueExtrusion,
		targetQueueExtrusion: m.targetQueueExtrusion,

		isCancelled: m.isCancelled,
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
		Query:             m.Query,

		sourceIndexPairProgress: m.sourceIndexPairProgress,
		targetIndexPairProgress: m.targetIndexPairProgress,
		pairProgress:            m.pairProgress,

		sourceQueueExtrusion: m.sourceQueueExtrusion,
		targetQueueExtrusion: m.targetQueueExtrusion,

		isCancelled: m.isCancelled,
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
		Query:             m.Query,

		sourceIndexPairProgress: m.sourceIndexPairProgress,
		targetIndexPairProgress: m.targetIndexPairProgress,
		pairProgress:            m.pairProgress,

		sourceQueueExtrusion: m.sourceQueueExtrusion,
		targetQueueExtrusion: m.targetQueueExtrusion,

		isCancelled: m.isCancelled,
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
		Query:             m.Query,

		sourceIndexPairProgress: m.sourceIndexPairProgress,
		targetIndexPairProgress: m.targetIndexPairProgress,
		pairProgress:            m.pairProgress,

		sourceQueueExtrusion: m.sourceQueueExtrusion,
		targetQueueExtrusion: m.targetQueueExtrusion,

		isCancelled: m.isCancelled,
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
		Query:             m.Query,

		sourceIndexPairProgress: m.sourceIndexPairProgress,
		targetIndexPairProgress: m.targetIndexPairProgress,
		pairProgress:            m.pairProgress,

		sourceQueueExtrusion: m.sourceQueueExtrusion,
		targetQueueExtrusion: m.targetQueueExtrusion,

		isCancelled: m.isCancelled,
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
		Query:             m.Query,

		sourceIndexPairProgress: m.sourceIndexPairProgress,
		targetIndexPairProgress: m.targetIndexPairProgress,
		pairProgress:            m.pairProgress,

		sourceQueueExtrusion: m.sourceQueueExtrusion,
		targetQueueExtrusion: m.targetQueueExtrusion,

		isCancelled: m.isCancelled,
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
		Query:             m.Query,

		sourceIndexPairProgress: m.sourceIndexPairProgress,
		targetIndexPairProgress: m.targetIndexPairProgress,
		pairProgress:            m.pairProgress,

		sourceQueueExtrusion: m.sourceQueueExtrusion,
		targetQueueExtrusion: m.targetQueueExtrusion,

		isCancelled: m.isCancelled,
	}
}

func (m *Migrator) WithQuery(query string) *Migrator {
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
		IndexFilePair:     m.IndexFilePair,
		IndexTemplate:     m.IndexTemplate,
		Query:             query,

		sourceIndexPairProgress: m.sourceIndexPairProgress,
		targetIndexPairProgress: m.targetIndexPairProgress,
		pairProgress:            m.pairProgress,

		sourceQueueExtrusion: m.sourceQueueExtrusion,
		targetQueueExtrusion: m.targetQueueExtrusion,

		isCancelled: m.isCancelled,
	}
}

func (m *Migrator) CopyIndexSettings(force bool) error {
	if m.err != nil {
		return errors.WithStack(m.err)
	}

	err := m.buildIndexPairContext()
	if err != nil {
		return errors.WithStack(err)
	}

	if force {
		if err := m.copyIndexSettings(m.IndexPair.TargetIndex, force); err != nil {
			return errors.WithStack(err)
		}
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

func (m *Migrator) mergeQueryMap(ids []string, subQueryMap map[string]interface{}) map[string]interface{} {
	query := make(map[string]interface{})
	if len(ids) > 0 {
		query = map[string]interface{}{
			"terms": map[string]interface{}{
				"_id": ids,
			},
		}
	}
	if len(subQueryMap) > 0 {
		query = lo.Assign(query, subQueryMap)
	}

	return query
}

func (m *Migrator) getQueryMap() map[string]interface{} {
	subQuery := make(map[string]interface{})
	if lo.IsNotEmpty(m.Query) {
		_ = json.Unmarshal([]byte(m.Query), &subQuery)
	}
	return m.mergeQueryMap(m.Ids, subQuery)
}

func (m *Migrator) SyncDiff() (*DiffResult, error) {
	if m.err != nil {
		return nil, errors.WithStack(m.err)
	}

	existed, err := m.TargetES.IndexExisted(m.IndexPair.TargetIndex)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !existed {
		return nil, utils.NewCustomError(utils.NonIndexExisted, "target index %s not existed", m.IndexPair.TargetIndex)
	}

	err = m.buildIndexPairContext()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var errs utils.Errs
	diffResult, err := m.compare()
	if err != nil {
		errs.Add(err)
	}

	if len(diffResult.CreateDocs) > 0 {
		if err := m.syncUpsert(m.mergeQueryMap(diffResult.CreateDocs, nil), es2.OperationCreate); err != nil {
			errs.Add(errors.WithStack(err))
		}
	}

	if len(diffResult.UpdateDocs) > 0 {
		if err := m.syncUpsert(m.mergeQueryMap(diffResult.UpdateDocs, nil), es2.OperationUpdate); err != nil {
			errs.Add(errors.WithStack(err))
		}
	}

	if len(diffResult.DeleteDocs) > 0 {
		if err := m.syncUpsert(m.mergeQueryMap(diffResult.DeleteDocs, nil), es2.OperationDelete); err != nil {
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

func (m *Migrator) getKeywordFields() ([]string, error) {
	sourceEsFieldMap := utils.GetCtxKeySourceFieldMap(m.ctx)

	targetEsFieldMap := utils.GetCtxKeyTargetFieldMap(m.ctx)

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

	utils.GoRecovery(m.ctx, func() {
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
	err := m.buildIndexPairContext()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	keywordFields, err := m.getKeywordFields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	errCh := make(chan error)
	errsCh := m.handleMultipleErrors(errCh)

	queryMap := m.getQueryMap()

	sourceDocCh, sourceTotal := m.search(m.SourceES, m.IndexPair.SourceIndex, queryMap, keywordFields, errCh, true)
	m.sourceIndexPairProgress.Reset(utils.ProgressNameCompareSourceIndexPair, sourceTotal)

	targetDocCh, targetTotal := m.search(m.TargetES, m.IndexPair.TargetIndex, queryMap, keywordFields, errCh, true)
	m.targetIndexPairProgress.Reset(utils.ProgressNameCompareTargetIndexPair, targetTotal)

	m.sourceQueueExtrusion.Reset(utils.ExtrusionNameCompareSourceIndexPair, cast.ToUint64(m.BufferCount))
	m.targetQueueExtrusion.Reset(utils.ExtrusionNameCompareTargetIndexPair, cast.ToUint64(m.BufferCount))
	var (
		diffResult DiffResult
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

				m.sourceQueueExtrusion.Set(cast.ToUint64(len(sourceDocCh)))
				m.targetQueueExtrusion.Set(cast.ToUint64(len(targetDocCh)))

				if !sourceOk && !targetOk {
					break
				}

				if *m.isCancelled {
					break
				}

				if sourceResult != nil {
					m.sourceIndexPairProgress.Increment(1)
					sourceDocHashMap.Store(sourceResult.ID, sourceResult.Hash)
				}

				if targetResult != nil {
					m.targetIndexPairProgress.Increment(1)
					targetDocHashMap.Store(targetResult.ID, targetResult.Hash)
				}

				if time.Now().Sub(lastPrintTime) > everyLogTime {
					m.sourceIndexPairProgress.Show(m.ctx)
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

	existed, err := m.TargetES.IndexExisted(m.IndexPair.TargetIndex)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !existed {
		return nil, utils.NewCustomError(utils.NonIndexExisted, "target index %s not existed", m.IndexPair.TargetIndex)
	}

	diffResult, err := m.compare()
	return diffResult, errors.WithStack(err)
}

func (m *Migrator) Sync(force bool) error {
	if m.err != nil {
		return errors.WithStack(m.err)
	}

	err := m.buildIndexPairContext()
	if err != nil {
		return errors.WithStack(err)
	}

	if force {
		_ = m.copyIndexSettings(m.IndexPair.TargetIndex, force)
	}
	if err := m.syncUpsert(m.getQueryMap(), es2.OperationCreate); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *Migrator) searchSingleSlice(wg *sync.WaitGroup, es es2.ES,
	index string, query map[string]interface{}, sortFields []string,
	sliceId *uint, sliceSize *uint, docCh chan *es2.Doc, errCh chan error, needHash bool) {

	utils.GoRecovery(m.ctx, func() {
		var (
			scrollResult *es2.ScrollResult
			err          error
		)
		defer func() {
			wg.Done()
			if scrollResult != nil {
				_ = es.ClearScroll(scrollResult.ScrollId)
			}
		}()

		func() {
			scrollResult, err = es.NewScroll(m.ctx, index, &es2.ScrollOption{
				Query:      query,
				SortFields: sortFields,
				ScrollSize: m.ScrollSize,
				ScrollTime: m.ScrollTime,
				SliceId:    sliceId,
				SliceSize:  sliceSize,
			})

			if err != nil {
				errCh <- errors.WithStack(err)
			}

			if scrollResult == nil {
				return
			}

		}()

		for {
			if scrollResult == nil || len(scrollResult.Docs) <= 0 {
				break
			}

			if *m.isCancelled {
				break
			}

			scrollResult.Docs = lop.Map(scrollResult.Docs, func(doc *es2.Doc, _ int) *es2.Doc {
				var fixErr error
				doc, fixErr = es2.FixDoc(m.ctx, doc)
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

			if scrollResult, err = es.NextScroll(m.ctx, scrollResult.ScrollId, m.ScrollTime); err != nil {
				errCh <- errors.WithStack(err)
			}
		}
	})
}

func (m *Migrator) search(es es2.ES, index string, query map[string]interface{},
	sortFields []string, errCh chan error, needHash bool) (chan *es2.Doc, uint64) {
	docCh := make(chan *es2.Doc, m.BufferCount)
	var wg sync.WaitGroup

	total, err := es.Count(m.ctx, index)
	if err != nil {
		errCh <- errors.WithStack(err)
		close(errCh)
		close(docCh)
		return nil, 0
	}

	if m.SliceSize <= 1 {
		wg.Add(1)
		m.searchSingleSlice(&wg, es, index, query, sortFields, nil, nil, docCh, errCh, needHash)
	} else {
		for i := uint(0); i < m.SliceSize; i++ {
			idx := i
			wg.Add(1)
			m.searchSingleSlice(&wg, es, index, query, sortFields, &idx, &m.SliceSize, docCh, errCh, needHash)
		}
	}
	utils.GoRecovery(m.ctx, func() {
		wg.Wait()
		close(docCh)
	})

	return docCh, total
}

func (m *Migrator) singleBulkWorker(docCh <-chan *es2.Doc, index string, operation es2.Operation, errCh chan error) {
	var buf bytes.Buffer

	lastPrintTime := time.Now()
	for {
		v, ok := <-docCh
		if !ok {
			break
		}

		if *m.isCancelled {
			break
		}

		v.Op = operation
		m.sourceIndexPairProgress.Increment(1)
		m.sourceQueueExtrusion.Set(cast.ToUint64(len(docCh)))
		if time.Now().Sub(lastPrintTime) > everyLogTime {
			m.sourceIndexPairProgress.Show(m.ctx)
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
		}

		if buf.Len() >= cast.ToInt(m.ActionSize)*1024*1024 {
			if err := m.TargetES.Bulk(&buf); err != nil {
				errCh <- errors.WithStack(err)
			}
			buf.Reset()
		}
	}

	if buf.Len() > 0 && !*m.isCancelled {
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
func (m *Migrator) bulkWorker(docCh <-chan *es2.Doc, index string, operation es2.Operation, errCh chan error) {
	var wg sync.WaitGroup

	if m.ActionParallelism <= 1 {
		m.singleBulkWorker(docCh, index, operation, errCh)
	}

	wg.Add(cast.ToInt(m.ActionParallelism))
	for i := 0; i < cast.ToInt(m.ActionParallelism) && !*m.isCancelled; i++ {
		utils.GoRecovery(m.ctx, func() {
			defer wg.Done()
			m.singleBulkWorker(docCh, index, operation, errCh)
		})
	}

	wg.Wait()
}

func (m *Migrator) singleBulkFileWorker(ctx context.Context, doc <-chan *es2.Doc, filepath string, errCh chan error) {
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

		if *m.isCancelled {
			break
		}

		m.sourceIndexPairProgress.Increment(1)
		m.sourceQueueExtrusion.Set(cast.ToUint64(len(doc)))

		if time.Now().Sub(lastPrintTime) > everyLogTime {
			m.sourceIndexPairProgress.Show(ctx)
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

	if buf.Len() > 0 && !*m.isCancelled {
		if _, err := f.WriteString(buf.String()); err != nil {
			errCh <- errors.WithStack(err)
		}
		_ = writer.Flush()
		buf.Reset()
	}

}

func (m *Migrator) bulkFileWorker(ctx context.Context, doc <-chan *es2.Doc, files []string, errCh chan error) {
	var wg sync.WaitGroup

	wg.Add(len(files))
	for _, file := range files {
		utils.GoRecovery(ctx, func() {
			defer wg.Done()
			m.singleBulkFileWorker(ctx, doc, file, errCh)
		})

		if *m.isCancelled {
			break
		}
	}
	wg.Wait()

	m.sourceIndexPairProgress.Finish(ctx)
}

func (m *Migrator) syncUpsert(query map[string]interface{}, operation es2.Operation) error {
	errCh := make(chan error)
	errsCh := m.handleMultipleErrors(errCh)

	var (
		docCh chan *es2.Doc
		total uint64
	)
	if operation == es2.OperationDelete {
		docCh, total = m.search(m.TargetES, m.IndexPair.SourceIndex, query, nil, errCh, false)
		m.sourceIndexPairProgress.Reset(utils.ProgressNameDeleteSourceIndexPair, total)
		m.sourceQueueExtrusion.Reset(utils.ExtrusionNameDeleteTargetIndexPair, cast.ToUint64(m.BufferCount))
	} else {
		docCh, total = m.search(m.SourceES, m.IndexPair.SourceIndex, query, nil, errCh, false)
		m.sourceIndexPairProgress.Reset(utils.ProgressNameUpsertSourceIndexPair, total)
		m.sourceQueueExtrusion.Reset(utils.ExtrusionNameUpsertTargetIndexPair, cast.ToUint64(m.BufferCount))
	}
	m.bulkWorker(docCh, m.IndexPair.TargetIndex, operation, errCh)
	close(errCh)
	errs := <-errsCh
	if errs.Len() > 0 {
		m.sourceIndexPairProgress.Fail(m.ctx)
	}
	return errs.Ret()
}

func (m *Migrator) copyIndexSettings(targetIndex string, force bool) error {
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

	sourceESSetting := utils.GetCtxKeySourceIndexSetting(m.ctx).(es2.IESSettings)

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
	} else if es2.ClusterVersionLte5(m.TargetES.GetClusterVersion()) {
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
	} else if es2.ClusterVersionLte5(m.TargetES.GetClusterVersion()) {
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
	utils.GoRecovery(ctx, func() {
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
	settingFile := fmt.Sprintf("%s/setting.json", m.IndexFilePair.IndexFileDir)
	settingBytes, err := os.ReadFile(settingFile)
	if err != nil {
		return m.ctx, nil, errors.WithStack(err)
	}
	settingMap := make(map[string]interface{})
	if err := json.Unmarshal(settingBytes, &settingMap); err != nil {
		return m.ctx, nil, errors.WithStack(err)
	}

	indexFileSetting, err := LoadIndexFileSettingFromMap(settingMap)
	if err != nil {
		return m.ctx, nil, errors.WithStack(err)
	}

	m.ctx = utils.SetCtxKeySourceObject(m.ctx, m.IndexFilePair.IndexFileDir)
	m.ctx = utils.SetCtxKeyTargetObject(m.ctx, m.IndexFilePair.Index)

	m.ctx = utils.SetCtxKeySourceESVersion(m.ctx, indexFileSetting.ESVersion)
	m.ctx = utils.SetCtxKeySourceIndexSetting(m.ctx, indexFileSetting.Settings)
	m.ctx = utils.SetCtxKeySourceFieldMap(m.ctx, indexFileSetting.Settings.GetFieldMap())
	m.ctx = m.addDateTimeFixFields(m.ctx, indexFileSetting.Settings.GetFieldMap())

	return m.ctx, indexFileSetting, nil
}

func (m *Migrator) buildExportIndexFilePairContext() (context.Context, error) {
	ctx := utils.SetCtxKeySourceObject(m.ctx, m.IndexFilePair.Index)
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

	if force {
		_ = m.copyIndexSettings(m.IndexFilePair.Index, force)
	}

	errCh := make(chan error)
	errsCh := m.handleMultipleErrors(errCh)

	var (
		docCh chan *es2.Doc
	)

	docCh = m.scrollFile(ctx, indexFileSetting, errCh)
	m.sourceIndexPairProgress.Reset(utils.ProgressNameImportSourceIndexPair, indexFileSetting.Total)
	m.sourceQueueExtrusion.Reset(utils.ExtrusionNameImportSourceIndexPair, cast.ToUint64(m.BufferCount))

	m.bulkWorker(docCh, m.IndexFilePair.Index, es2.OperationCreate, errCh)
	close(errCh)
	errs := <-errsCh
	if errs.Len() > 0 {
		m.sourceIndexPairProgress.Fail(ctx)
	} else {
		m.sourceIndexPairProgress.Finish(ctx)
	}
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

	query := m.getQueryMap()
	docCh, total = m.search(m.SourceES, m.IndexFilePair.Index, query, nil, errCh, false)
	m.sourceIndexPairProgress.Reset(utils.ProgressNameExportSourceIndexPair, total)
	m.sourceQueueExtrusion.Reset(utils.ExtrusionNameExportSourceIndexPair, cast.ToUint64(m.BufferCount))

	m.bulkFileWorker(ctx, docCh, indexFileSetting.Files, errCh)
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
