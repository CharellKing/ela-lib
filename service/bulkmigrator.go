package service

import (
	"context"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/alitto/pond"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
)

type BulkMigrator struct {
	ctx context.Context

	SourceES es2.ES
	TargetES es2.ES

	Parallelism uint

	IndexPairMap map[string]*config.IndexPair

	IndexFilePairMap map[string]*config.IndexFilePair

	Error error

	ScrollSize uint

	ScrollTime uint

	SliceSize uint

	BufferCount uint

	ActionSize uint

	ActionParallelism uint

	Ids []string

	Pattern string
}

func NewBulkMigratorWithES(ctx context.Context, sourceES, targetES es2.ES) *BulkMigrator {
	ctx = utils.SetCtxKeySourceESVersion(ctx, sourceES.GetClusterVersion())
	ctx = utils.SetCtxKeyTargetESVersion(ctx, targetES.GetClusterVersion())

	return &BulkMigrator{
		ctx:               ctx,
		SourceES:          sourceES,
		TargetES:          targetES,
		Parallelism:       defaultParallelism,
		IndexPairMap:      make(map[string]*config.IndexPair),
		Error:             nil,
		ScrollSize:        defaultScrollSize,
		ScrollTime:        defaultScrollTime,
		SliceSize:         defaultSliceSize,
		BufferCount:       defaultBufferCount,
		ActionSize:        defaultActionSize,
		ActionParallelism: defaultActionParallelism,
	}
}

func NewBulkMigrator(ctx context.Context, srcConfig *config.ESConfig, dstConfig *config.ESConfig) (*BulkMigrator, error) {
	srcES, err := es2.NewESV0(srcConfig).GetES()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	dstES, err := es2.NewESV0(dstConfig).GetES()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return NewBulkMigratorWithES(ctx, srcES, dstES), nil
}

func (m *BulkMigrator) GetCtx() context.Context {
	return m.ctx
}

func (m *BulkMigrator) getIndexPairKey(indexPair *config.IndexPair) string {
	return fmt.Sprintf("%s:%s", indexPair.SourceIndex, indexPair.TargetIndex)
}

func (m *BulkMigrator) WithIndexPairs(indexPairs ...*config.IndexPair) *BulkMigrator {
	if m.Error != nil {
		return m
	}

	newBulkMigrator := &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       m.Parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionParallelism: m.ActionParallelism,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		IndexFilePairMap:  m.IndexFilePairMap,
		Pattern:           m.Pattern,
	}

	newIndexPairsMap := make(map[string]*config.IndexPair)
	for _, indexPair := range indexPairs {
		indexPairKey := m.getIndexPairKey(indexPair)
		if _, ok := newIndexPairsMap[indexPairKey]; !ok {
			newIndexPairsMap[indexPairKey] = indexPair
		}
	}

	if len(newIndexPairsMap) > 0 {
		newBulkMigrator.IndexPairMap = lo.Assign(newBulkMigrator.IndexPairMap, newIndexPairsMap)
	}
	return newBulkMigrator
}

func (m *BulkMigrator) WithIndexFilePairs(indexFilePairs ...*config.IndexFilePair) *BulkMigrator {
	if m.Error != nil {
		return m
	}

	newBulkMigrator := &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       m.Parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionParallelism: m.ActionParallelism,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		IndexFilePairMap:  m.IndexFilePairMap,
		Pattern:           m.Pattern,
	}

	newIndexPairsMap := make(map[string]*config.IndexFilePair)
	for _, importIndexFilePair := range indexFilePairs {
		if _, ok := newIndexPairsMap[importIndexFilePair.Index]; !ok {
			newIndexPairsMap[importIndexFilePair.Index] = importIndexFilePair
		}
	}

	if len(newIndexPairsMap) > 0 {
		newBulkMigrator.IndexFilePairMap = lo.Assign(newBulkMigrator.IndexFilePairMap, newIndexPairsMap)
	}
	return newBulkMigrator
}

func (m *BulkMigrator) WithScrollSize(scrollSize uint) *BulkMigrator {
	if m.Error != nil {
		return m
	}

	if scrollSize == 0 {
		scrollSize = defaultScrollSize
	}

	return &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       m.Parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        scrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Pattern:           m.Pattern,
	}
}

func (m *BulkMigrator) WithScrollTime(scrollTime uint) *BulkMigrator {
	if m.Error != nil {
		return m
	}

	if scrollTime == 0 {
		scrollTime = defaultScrollTime
	}
	return &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       m.Parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        scrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Pattern:           m.Pattern,
	}
}

func (m *BulkMigrator) WithSliceSize(sliceSize uint) *BulkMigrator {
	if m.Error != nil {
		return m
	}

	if sliceSize == 0 {
		sliceSize = defaultSliceSize
	}
	return &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       m.Parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         sliceSize,
		BufferCount:       m.BufferCount,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Pattern:           m.Pattern,
	}
}

func (m *BulkMigrator) WithBufferCount(bufferCount uint) *BulkMigrator {
	if m.Error != nil {
		return m
	}

	if bufferCount == 0 {
		bufferCount = defaultBufferCount
	}
	return &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       m.Parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       bufferCount,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Pattern:           m.Pattern,
	}
}

func (m *BulkMigrator) WithActionParallelism(actionParallelism uint) *BulkMigrator {
	if m.Error != nil {
		return m
	}

	if actionParallelism == 0 {
		actionParallelism = defaultActionParallelism
	}

	return &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       m.Parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionSize:        m.ActionSize,
		Ids:               m.Ids,
		ActionParallelism: actionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Pattern:           m.Pattern,
	}
}

func (m *BulkMigrator) WithActionSize(actionSize uint) *BulkMigrator {
	if m.Error != nil {
		return m
	}

	if actionSize == 0 {
		actionSize = defaultActionSize
	}

	return &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       m.Parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionSize:        actionSize,
		Ids:               m.Ids,
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Pattern:           m.Pattern,
	}
}

func (m *BulkMigrator) filterIndexes(pattern string) ([]string, error) {
	ignoreSystemIndex := utils.GetCtxKeyIgnoreSystemIndex(m.ctx)

	indexes, err := m.SourceES.GetIndexes()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var filteredIndexes []string
	for _, index := range indexes {
		if ignoreSystemIndex && strings.HasPrefix(index, ".") {
			continue
		}

		ok, err := regexp.Match(pattern, []byte(index))
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if ok {
			filteredIndexes = append(filteredIndexes, index)
		}
	}
	return filteredIndexes, nil
}

func (m *BulkMigrator) WithPatternIndexes(pattern string) *BulkMigrator {
	if m.Error != nil {
		return m
	}

	newBulkMigrator := &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       m.Parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		Ids:               m.Ids,
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Pattern:           pattern,
	}

	return newBulkMigrator
}

func (m *BulkMigrator) WithParallelism(parallelism uint) *BulkMigrator {
	if m.Error != nil {
		return m
	}

	if parallelism == 0 {
		parallelism = defaultParallelism
	}
	return &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		Ids:               m.Ids,
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Pattern:           m.Pattern,
	}
}

func (m *BulkMigrator) WithIds(ids []string) *BulkMigrator {
	if m.Error != nil {
		return m
	}

	return &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       m.Parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionSize:        m.ActionSize,
		Ids:               ids,
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Pattern:           m.Pattern,
	}
}

func (m *BulkMigrator) Sync(force bool) error {
	newBulkMigrator := m.getIndexPairsFromPattern()
	if newBulkMigrator.Error != nil {
		return errors.WithStack(newBulkMigrator.Error)
	}

	bar := utils.NewProgressBar(newBulkMigrator.ctx, "All tasks", "", len(newBulkMigrator.IndexPairMap))
	defer bar.Finish()

	newBulkMigrator.parallelRun(func(migrator *Migrator) {
		defer bar.Increment()
		if err := migrator.Sync(force); err != nil {
			utils.GetLogger(migrator.GetCtx()).Errorf("sync %+v", err)
		}
	})
	return nil
}

func (m *BulkMigrator) SyncDiff() (map[string]*DiffResult, error) {
	newBulkMigrator := m.getIndexPairsFromPattern()
	if newBulkMigrator.Error != nil {
		return nil, errors.WithStack(newBulkMigrator.Error)
	}

	bar := utils.NewProgressBar(newBulkMigrator.ctx, "All Task", "", len(newBulkMigrator.IndexPairMap))
	defer bar.Finish()

	var diffMap sync.Map
	newBulkMigrator.parallelRun(func(migrator *Migrator) {
		defer bar.Increment()
		diffResult, err := migrator.SyncDiff()
		if err != nil {
			utils.GetLogger(migrator.GetCtx()).Errorf("syncDiff %+v", err)
			return
		}
		if diffResult.HasDiff() {
			diffMap.Store(newBulkMigrator.getIndexPairKey(migrator.IndexPair), diffResult)
		} else {
			utils.GetLogger(migrator.GetCtx()).Info("no difference")
		}
	})

	result := make(map[string]*DiffResult)
	diffMap.Range(func(key, value interface{}) bool {
		keyStr := cast.ToString(key)
		result[keyStr] = value.(*DiffResult)
		return true
	})

	return result, nil
}

func (m *BulkMigrator) Compare() (map[string]*DiffResult, error) {
	newBulkMigrator := m.getIndexPairsFromPattern()
	if newBulkMigrator.Error != nil {
		return nil, errors.WithStack(newBulkMigrator.Error)
	}

	var diffMap sync.Map

	bar := utils.NewProgressBar(newBulkMigrator.ctx, "All Task", "", len(newBulkMigrator.IndexPairMap))
	defer bar.Finish()

	newBulkMigrator.parallelRun(func(migrator *Migrator) {
		defer bar.Increment()
		diffResult, err := migrator.Compare()
		if err != nil {
			utils.GetLogger(newBulkMigrator.GetCtx()).Errorf("compare %+v", err)
			return
		}
		if diffResult.HasDiff() {
			diffMap.Store(newBulkMigrator.getIndexPairKey(migrator.IndexPair), diffResult)
		} else {
			utils.GetLogger(migrator.GetCtx()).Info("no difference")
		}
	})

	result := make(map[string]*DiffResult)

	diffMap.Range(func(key, value interface{}) bool {
		keyStr := cast.ToString(key)
		result[keyStr] = value.(*DiffResult)
		return true
	})

	return result, nil
}

func (m *BulkMigrator) CopyIndexSettings(force bool) error {
	newBulkMigrator := m.getIndexPairsFromPattern()
	if newBulkMigrator.Error != nil {
		return errors.WithStack(newBulkMigrator.Error)
	}

	bar := utils.NewProgressBar(newBulkMigrator.ctx, "All Task", "", len(newBulkMigrator.IndexPairMap))
	defer bar.Finish()

	newBulkMigrator.parallelRun(func(migrator *Migrator) {
		defer bar.Increment()
		if err := migrator.CopyIndexSettings(force); err != nil {
			utils.GetLogger(migrator.GetCtx()).Errorf("copyIndexSettings %+v", err)
		}
	})
	return nil
}

func (m *BulkMigrator) clone() *BulkMigrator {
	return &BulkMigrator{
		ctx:               m.ctx,
		SourceES:          m.SourceES,
		TargetES:          m.TargetES,
		Parallelism:       m.Parallelism,
		IndexPairMap:      m.IndexPairMap,
		Error:             m.Error,
		ScrollSize:        m.ScrollSize,
		ScrollTime:        m.ScrollTime,
		SliceSize:         m.SliceSize,
		BufferCount:       m.BufferCount,
		ActionSize:        m.ActionSize,
		ActionParallelism: m.ActionParallelism,
	}
}

func (m *BulkMigrator) getIndexPairsFromPattern() *BulkMigrator {
	if m.Error != nil {
		return m
	}

	var newBulkMigrator = m.clone()
	var filterIndexes []string

	filterIndexes, newBulkMigrator.Error = m.filterIndexes(m.Pattern)
	if newBulkMigrator.Error != nil {
		return newBulkMigrator
	}

	newIndexPairsMap := make(map[string]*config.IndexPair)
	for _, index := range filterIndexes {
		indexPair := &config.IndexPair{
			SourceIndex: index,
			TargetIndex: index,
		}

		newIndexPairKey := m.getIndexPairKey(indexPair)
		if _, ok := newBulkMigrator.IndexPairMap[newIndexPairKey]; !ok {
			newIndexPairsMap[newIndexPairKey] = indexPair
		}
	}

	if len(newIndexPairsMap) > 0 {
		newBulkMigrator.IndexPairMap = lo.Assign(newBulkMigrator.IndexPairMap, newIndexPairsMap)
	}
	return newBulkMigrator
}

func (m *BulkMigrator) getIndexFilePairFromPattern() *BulkMigrator {
	if m.Error != nil {
		return m
	}

	var newBulkMigrator = m.clone()
	var filterIndexes []string

	filterIndexes, newBulkMigrator.Error = m.filterIndexes(m.Pattern)
	if newBulkMigrator.Error != nil {
		return newBulkMigrator
	}

	newIndexPairsMap := make(map[string]*config.IndexFilePair)
	for _, index := range filterIndexes {

		indexPair := &config.IndexFilePair{
			Index: index,
			File:  fmt.Sprintf("%s/%s", m.Pattern, index),
		}
		if _, ok := newBulkMigrator.IndexPairMap[index]; !ok {
			newIndexPairsMap[index] = indexPair
		}
	}

	if len(newIndexPairsMap) > 0 {
		newBulkMigrator.IndexFilePairMap = lo.Assign(newBulkMigrator.IndexFilePairMap, newIndexPairsMap)
	}
	return newBulkMigrator
}

func (m *BulkMigrator) parallelRun(callback func(migrator *Migrator)) {
	pool := pond.New(cast.ToInt(m.Parallelism), len(m.IndexPairMap))
	finishCount := atomic.Int32{}

	for _, indexPair := range m.IndexPairMap {
		newMigrator := NewMigrator(m.ctx, m.SourceES, m.TargetES)
		newMigrator = newMigrator.WithIndexPair(*indexPair).
			WithScrollSize(m.ScrollSize).
			WithScrollTime(m.ScrollTime).
			WithSliceSize(m.SliceSize).
			WithBufferCount(m.BufferCount).
			WithActionParallelism(m.ActionParallelism).
			WithActionSize(m.ActionSize).
			WithIds(m.Ids)

		pool.Submit(func() {
			callback(newMigrator)
			finishCount.Add(1)
			utils.GetLogger(newMigrator.GetCtx()).Infof("task progress %0.4f (%d, %d)", float64(finishCount.Load())/float64(len(m.IndexPairMap)), finishCount.Load(), len(m.IndexPairMap))
		})
	}
	pool.StopAndWait()
}

func (m *BulkMigrator) Import() error {
	newBulkMigrator := m.getIndexFilePairFromPattern()
	if newBulkMigrator.Error != nil {
		return errors.WithStack(newBulkMigrator.Error)
	}

	bar := utils.NewProgressBar(m.ctx, "All Task", "", len(m.IndexFilePairMap))
	defer bar.Finish()

	m.parallelRun(func(migrator *Migrator) {
		defer bar.Increment()
		if err := migrator.Import(); err != nil {
			utils.GetLogger(migrator.GetCtx()).Errorf("import %+v", err)
		}
	})
	return nil
}

func (m *BulkMigrator) Export() error {
	newBulkMigrator := m.getIndexFilePairFromPattern()
	if newBulkMigrator.Error != nil {
		return errors.WithStack(newBulkMigrator.Error)
	}

	bar := utils.NewProgressBar(newBulkMigrator.ctx, "All Task", "", len(newBulkMigrator.IndexFilePairMap))
	defer bar.Finish()

	newBulkMigrator.parallelRun(func(migrator *Migrator) {
		defer bar.Increment()
		if err := migrator.Export(); err != nil {
			utils.GetLogger(migrator.GetCtx()).Errorf("export %+v", err)
		}
	})
	return nil
}
