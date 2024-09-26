package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/bytedance/gopkg/collection/skipmap"
	"github.com/pkg/errors"
	lop "github.com/samber/lo/parallel"
	"github.com/spf13/cast"
	"hash/fnv"
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
const defaultWriteParallel = 20
const defaultWriteSize = 10 // MB
const defaultCompareParallel = 20

type Migrator struct {
	err error

	ctx context.Context

	SourceES es2.ES
	TargetES es2.ES

	IndexPair config.IndexPair

	ScrollSize uint

	ScrollTime uint

	SliceSize uint

	BufferCount uint

	CompareParallel uint

	WriteParallel uint

	WriteSize uint

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
	ctx = utils.SetCtxKeySourceESVersion(ctx, srcES.GetClusterVersion())
	ctx = utils.SetCtxKeyTargetESVersion(ctx, dstES.GetClusterVersion())

	return &Migrator{
		err:             nil,
		ctx:             ctx,
		SourceES:        srcES,
		TargetES:        dstES,
		ScrollSize:      defaultScrollSize,
		ScrollTime:      defaultScrollTime,
		SliceSize:       defaultSliceSize,
		BufferCount:     defaultBufferCount,
		WriteParallel:   defaultWriteParallel,
		WriteSize:       defaultWriteSize,
		CompareParallel: defaultCompareParallel,
	}
}

func (m *Migrator) GetCtx() context.Context {
	return m.ctx
}

func (m *Migrator) addDateTimeFixFields(ctx context.Context, fieldMap map[string]interface{}) context.Context {
	if !strings.HasPrefix(m.SourceES.GetClusterVersion(), "5.") {
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

func (m *Migrator) WithIndexPair(indexPair config.IndexPair) *Migrator {
	if m.err != nil {
		return m
	}

	ctx := utils.SetCtxKeySourceIndex(m.ctx, indexPair.SourceIndex)
	ctx = utils.SetCtxKeyTargetIndex(ctx, indexPair.TargetIndex)

	var err error
	sourceSetting, err := m.SourceES.GetIndexMappingAndSetting(indexPair.SourceIndex)
	if err != nil {
		m.err = errors.WithStack(err)
		return m
	}

	targetSetting, err := m.SourceES.GetIndexMappingAndSetting(indexPair.TargetIndex)
	if err != nil {
		m.err = errors.WithStack(err)
		return m
	}

	ctx = utils.SetCtxKeySourceIndexSetting(ctx, sourceSetting)
	ctx = utils.SetCtxKeyTargetIndexSetting(ctx, targetSetting)
	ctx = utils.SetCtxKeySourceFieldMap(ctx, sourceSetting.GetFieldMap())
	ctx = utils.SetCtxKeyTargetFieldMap(ctx, targetSetting.GetFieldMap())
	ctx = m.addDateTimeFixFields(ctx, sourceSetting.GetFieldMap())

	return &Migrator{
		err:             err,
		ctx:             ctx,
		SourceES:        m.SourceES,
		TargetES:        m.TargetES,
		IndexPair:       indexPair,
		ScrollSize:      m.ScrollSize,
		ScrollTime:      m.ScrollTime,
		SliceSize:       m.SliceSize,
		BufferCount:     m.BufferCount,
		WriteParallel:   m.WriteParallel,
		WriteSize:       m.WriteSize,
		Ids:             m.Ids,
		CompareParallel: m.CompareParallel,
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
		ctx:             m.ctx,
		SourceES:        m.SourceES,
		TargetES:        m.TargetES,
		IndexPair:       m.IndexPair,
		ScrollSize:      scrollSize,
		ScrollTime:      m.ScrollTime,
		SliceSize:       m.SliceSize,
		BufferCount:     m.BufferCount,
		WriteParallel:   m.WriteParallel,
		WriteSize:       m.WriteSize,
		Ids:             m.Ids,
		CompareParallel: m.CompareParallel,
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
		ctx:             m.ctx,
		SourceES:        m.SourceES,
		TargetES:        m.TargetES,
		IndexPair:       m.IndexPair,
		ScrollSize:      m.ScrollSize,
		ScrollTime:      scrollTime,
		SliceSize:       m.SliceSize,
		BufferCount:     m.BufferCount,
		WriteParallel:   m.WriteParallel,
		WriteSize:       m.WriteSize,
		Ids:             m.Ids,
		CompareParallel: m.CompareParallel,
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
		ctx:             m.ctx,
		SourceES:        m.SourceES,
		TargetES:        m.TargetES,
		IndexPair:       m.IndexPair,
		ScrollSize:      m.ScrollSize,
		ScrollTime:      m.ScrollTime,
		SliceSize:       sliceSize,
		BufferCount:     m.BufferCount,
		WriteParallel:   m.WriteParallel,
		WriteSize:       m.WriteSize,
		Ids:             m.Ids,
		CompareParallel: m.CompareParallel,
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
		ctx:             m.ctx,
		SourceES:        m.SourceES,
		TargetES:        m.TargetES,
		IndexPair:       m.IndexPair,
		ScrollSize:      m.ScrollSize,
		ScrollTime:      m.ScrollTime,
		SliceSize:       m.SliceSize,
		BufferCount:     sliceSize,
		WriteParallel:   m.WriteParallel,
		WriteSize:       m.WriteSize,
		Ids:             m.Ids,
		CompareParallel: m.CompareParallel,
	}
}

func (m *Migrator) WithWriteParallel(writeParallel uint) *Migrator {
	if m.err != nil {
		return m
	}

	if writeParallel <= 0 {
		writeParallel = defaultWriteParallel
	}
	return &Migrator{
		ctx:             m.ctx,
		SourceES:        m.SourceES,
		TargetES:        m.TargetES,
		IndexPair:       m.IndexPair,
		ScrollSize:      m.ScrollSize,
		ScrollTime:      m.ScrollTime,
		SliceSize:       m.SliceSize,
		BufferCount:     m.BufferCount,
		WriteParallel:   writeParallel,
		WriteSize:       m.WriteSize,
		Ids:             m.Ids,
		CompareParallel: m.CompareParallel,
	}
}

func (m *Migrator) WithWriteSize(writeSize uint) *Migrator {
	if m.err != nil {
		return m
	}

	if writeSize <= 0 {
		writeSize = defaultWriteSize
	}

	return &Migrator{
		ctx:             m.ctx,
		SourceES:        m.SourceES,
		TargetES:        m.TargetES,
		IndexPair:       m.IndexPair,
		ScrollSize:      m.ScrollSize,
		ScrollTime:      m.ScrollTime,
		SliceSize:       m.SliceSize,
		BufferCount:     m.BufferCount,
		WriteParallel:   m.WriteParallel,
		WriteSize:       writeSize,
		Ids:             m.Ids,
		CompareParallel: m.CompareParallel,
	}
}

func (m *Migrator) WithIds(ids []string) *Migrator {
	if m.err != nil {
		return m
	}

	return &Migrator{
		ctx:             m.ctx,
		SourceES:        m.SourceES,
		TargetES:        m.TargetES,
		IndexPair:       m.IndexPair,
		ScrollSize:      m.ScrollSize,
		ScrollTime:      m.ScrollTime,
		SliceSize:       m.SliceSize,
		BufferCount:     m.BufferCount,
		WriteParallel:   m.WriteParallel,
		WriteSize:       m.WriteSize,
		Ids:             ids,
		CompareParallel: m.CompareParallel,
	}
}

func (m *Migrator) WithCompareParallel(compareParallel uint) *Migrator {
	if m.err != nil {
		return m
	}

	if compareParallel <= 0 {
		compareParallel = defaultCompareParallel
	}
	return &Migrator{
		ctx:             m.ctx,
		SourceES:        m.SourceES,
		TargetES:        m.TargetES,
		IndexPair:       m.IndexPair,
		ScrollSize:      m.ScrollSize,
		ScrollTime:      m.ScrollTime,
		SliceSize:       m.SliceSize,
		BufferCount:     m.BufferCount,
		WriteParallel:   m.WriteParallel,
		WriteSize:       m.WriteSize,
		Ids:             m.Ids,
		CompareParallel: compareParallel,
	}
}

func (m *Migrator) CopyIndexSettings(force bool) error {
	if m.err != nil {
		return errors.WithStack(m.err)
	}

	existed, err := m.TargetES.IndexExisted(m.IndexPair.TargetIndex)
	if err != nil {
		return errors.WithStack(err)
	}

	if existed && !force {
		return nil
	}

	if existed {
		if err := m.TargetES.DeleteIndex(m.IndexPair.TargetIndex); err != nil {
			return errors.WithStack(err)
		}
	}

	if err := m.copyIndexSettings(); err != nil {
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

	var errs utils.Errs
	diffResult, err := m.compare()
	if err != nil {
		errs.Add(err)
	}

	if len(diffResult.CreateDocs) > 0 {
		utils.GetLogger(m.ctx).Debugf("sync with create docs: %+v", len(diffResult.CreateDocs))
		if err := m.syncUpsert(getQueryMap(diffResult.CreateDocs), es2.OperationCreate); err != nil {
			errs.Add(errors.WithStack(err))
		}
	}

	if len(diffResult.UpdateDocs) > 0 {
		utils.GetLogger(m.ctx).Debugf("sync with update docs: %+v", len(diffResult.UpdateDocs))
		if err := m.syncUpsert(getQueryMap(diffResult.UpdateDocs), es2.OperationUpdate); err != nil {
			errs.Add(errors.WithStack(err))
		}
	}

	if len(diffResult.DeleteDocs) > 0 {
		utils.GetLogger(m.ctx).Debugf("sync with delete docs: %+v", len(diffResult.DeleteDocs))
		if err := m.syncUpsert(getQueryMap(diffResult.DeleteDocs), es2.OperationDelete); err != nil {
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
	keywordFields, err := m.getKeywordFields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	errCh := make(chan error)
	errsCh := m.handleMultipleErrors(errCh)

	queryMap := getQueryMap(m.Ids)

	sourceDocCh, sourceTotal := m.search(m.SourceES, m.IndexPair.SourceIndex, queryMap, keywordFields, errCh, true)

	targetDocCh, targetTotal := m.search(m.TargetES, m.IndexPair.TargetIndex, queryMap, keywordFields, errCh, true)

	var (
		sourceCount atomic.Uint64
		targetCount atomic.Uint64
		diffResult  DiffResult
	)

	sourceDocHashMap := skipmap.NewString()
	targetDocHashMap := skipmap.NewString()

	lastPrintTime := time.Now()

	var wg sync.WaitGroup
	for i := uint(0); i < m.CompareParallel; i++ {
		wg.Add(1)
		utils.GoRecovery(m.GetCtx(), func() {
			defer wg.Done()
			bar := utils.NewProgressBar(m.ctx, "All Task", "diff", cast.ToInt(sourceTotal+targetTotal))
			defer bar.Finish()

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
					close(errCh)
					break
				}

				if sourceResult != nil {
					bar.Increment()
					sourceCount.Add(1)
					sourceDocHashMap.Store(sourceResult.ID, sourceResult.Hash)
				}

				if targetResult != nil {
					bar.Increment()
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

	utils.GetLogger(m.ctx).Debugf("sync with force: %+v", force)
	if err := m.CopyIndexSettings(force); err != nil {
		return errors.WithStack(err)
	}
	if err := m.syncUpsert(getQueryMap(m.Ids), es2.OperationCreate); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *Migrator) searchSingleSlice(wg *sync.WaitGroup, es es2.ES,
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
			scrollResult, err = es.NewScroll(m.ctx, index, &es2.ScrollOption{
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
			if scrollResult == nil {
				break
			}
			if needHash {
				lop.Map(scrollResult.Docs, func(doc *es2.Doc, _ int) *es2.Doc {
					doc.Hash = m.getDocHash(doc)
					return doc
				})
			}
			for _, doc := range scrollResult.Docs {
				docCh <- doc
			}

			if len(scrollResult.Docs) < cast.ToInt(m.ScrollSize) {
				break
			}
			if scrollResult, err = es.NextScroll(m.GetCtx(), scrollResult.ScrollId, m.ScrollTime); err != nil {
				utils.GetLogger(m.GetCtx()).Errorf("searchSingleSlice error: %+v", err)
				errCh <- errors.WithStack(err)
			}
		}
	})
}

func (m *Migrator) search(es es2.ES, index string, query map[string]interface{},
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
		m.searchSingleSlice(&wg, es, index, query, sortFields, nil, nil, docCh, errCh, needHash)
	} else {
		for i := uint(0); i < m.SliceSize; i++ {
			idx := i
			wg.Add(1)
			m.searchSingleSlice(&wg, es, index, query, sortFields, &idx, &m.SliceSize, docCh, errCh, needHash)
		}
	}
	utils.GoRecovery(m.GetCtx(), func() {
		wg.Wait()
		close(docCh)
	})

	return docCh, total
}

func (m *Migrator) singleBulkWorker(bar *utils.ProgressBar, doc <-chan *es2.Doc, total uint64, count *atomic.Uint64,
	operation es2.Operation, errCh chan error) {
	var buf bytes.Buffer

	lastPrintTime := time.Now()
	for {
		v, ok := <-doc
		if !ok {
			break
		}
		bar.Increment()
		count.Add(1)
		percent := cast.ToFloat32(count.Load()) / cast.ToFloat32(total)

		if time.Now().Sub(lastPrintTime) > everyLogTime {
			utils.GetLogger(m.GetCtx()).Infof("bulk progress %.4f (%d, %d, %d)",
				percent, count.Load(), total, len(doc))
			lastPrintTime = time.Now()
		}
		switch operation {
		case es2.OperationCreate:
			if err := m.TargetES.BulkBody(m.IndexPair.TargetIndex, &buf, v); err != nil {
				errCh <- errors.WithStack(err)
			}
		case es2.OperationUpdate:
			if err := m.TargetES.BulkBody(m.IndexPair.TargetIndex, &buf, v); err != nil {
				errCh <- errors.WithStack(err)
			}
		case es2.OperationDelete:
			if err := m.TargetES.BulkBody(m.IndexPair.TargetIndex, &buf, v); err != nil {
				errCh <- errors.WithStack(err)
			}
		default:
			utils.GetLogger(m.ctx).Error("unknown operation")
		}

		if buf.Len() >= cast.ToInt(m.WriteSize)*1024*1024 {
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
func (m *Migrator) bulkWorker(doc <-chan *es2.Doc, total uint64, operation es2.Operation, errCh chan error) {
	var wg sync.WaitGroup
	var count atomic.Uint64
	bar := utils.NewProgressBar(m.ctx, "All Tasks", fmt.Sprintf("bulk.%s", m.getOperationTitle(operation)),
		cast.ToInt(total))
	defer bar.Finish()

	if m.WriteParallel <= 1 {
		m.singleBulkWorker(bar, doc, total, &count, operation, errCh)
	}

	wg.Add(cast.ToInt(m.WriteParallel))
	for i := 0; i < cast.ToInt(m.WriteParallel); i++ {
		utils.GoRecovery(m.ctx, func() {
			defer wg.Done()
			m.singleBulkWorker(bar, doc, total, &count, operation, errCh)
		})
	}
	wg.Wait()
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
	} else {
		docCh, total = m.search(m.SourceES, m.IndexPair.SourceIndex, query, nil, errCh, false)
	}
	m.bulkWorker(docCh, total, operation, errCh)
	close(errCh)
	errs := <-errsCh
	return errs.Ret()
}

func (m *Migrator) copyIndexSettings() error {
	sourceESSetting := utils.GetCtxKeySourceIndexSetting(m.ctx).(es2.IESSettings)

	targetESSetting := m.GetTargetESSetting(sourceESSetting)

	if err := m.TargetES.CreateIndex(targetESSetting); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (m *Migrator) GetTargetESSetting(sourceESSetting es2.IESSettings) es2.IESSettings {
	if strings.HasPrefix(m.TargetES.GetClusterVersion(), "8.") {
		return sourceESSetting.ToTargetV8Settings(m.IndexPair.TargetIndex)
	} else if strings.HasPrefix(m.TargetES.GetClusterVersion(), "7.") {
		return sourceESSetting.ToTargetV7Settings(m.IndexPair.TargetIndex)
	} else if strings.HasPrefix(m.TargetES.GetClusterVersion(), "6.") {
		return sourceESSetting.ToTargetV6Settings(m.IndexPair.TargetIndex)
	} else if strings.HasPrefix(m.TargetES.GetClusterVersion(), "5.") {
		return sourceESSetting.ToTargetV5Settings(m.IndexPair.TargetIndex)
	}

	return nil
}
