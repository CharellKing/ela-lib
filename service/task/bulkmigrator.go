package task

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	es2 "github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/alitto/pond"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"os"
	"regexp"
	"strings"
	"sync"
)

type BulkMigrator struct {
	ctx context.Context

	SourceES es2.ES
	TargetES es2.ES

	Parallelism uint

	IndexPairMap map[string]*config.IndexPair

	IndexFilePairMap map[string]*config.IndexFilePair

	IndexTemplates map[string]*config.IndexTemplate

	Error error

	ScrollSize uint

	ScrollTime uint

	SliceSize uint

	BufferCount uint

	ActionSize uint

	ActionParallelism uint

	Ids []string

	Patterns []string

	IndexFileRoot string

	Query string

	taskProgress *utils.Progress

	isCancelled *bool
}

func NewBulkMigratorWithES(ctx context.Context, sourceES, targetES es2.ES, isCancelled *bool) *BulkMigrator {
	if lo.IsNotEmpty(sourceES) {
		ctx = utils.SetCtxKeySourceESVersion(ctx, sourceES.GetClusterVersion())
	}

	if lo.IsNotEmpty(targetES) {
		ctx = utils.SetCtxKeyTargetESVersion(ctx, targetES.GetClusterVersion())
	}

	taskProgress := utils.NewProgress(utils.ProgressNameTask, 0)
	ctx = utils.SetCtxKeyTaskProgress(ctx, taskProgress)
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

		taskProgress: taskProgress,
		isCancelled:  isCancelled,
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

	return NewBulkMigratorWithES(ctx, srcES, dstES, lo.ToPtr(false)), nil
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
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
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
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		Query:             m.Query,
		taskProgress:      m.taskProgress,
		isCancelled:       m.isCancelled,
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

func (m *BulkMigrator) WithIndexTemplates(indexTemplates ...*config.IndexTemplate) *BulkMigrator {
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
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
	}

	newIndexTemplateMap := make(map[string]*config.IndexTemplate)
	for _, indexTemplate := range indexTemplates {
		if _, ok := newIndexTemplateMap[indexTemplate.Name]; !ok {
			newIndexTemplateMap[indexTemplate.Name] = indexTemplate
		}
	}

	if len(newIndexTemplateMap) > 0 {
		newBulkMigrator.IndexTemplates = lo.Assign(newBulkMigrator.IndexTemplates, newIndexTemplateMap)
	}
	return newBulkMigrator
}

func (m *BulkMigrator) WithIndexFileRoot(indexFileRoot string) *BulkMigrator {
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
		Patterns:          m.Patterns,
		IndexFileRoot:     indexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
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
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
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
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
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
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
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
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
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
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
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
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
	}
}

func (m *BulkMigrator) filterIndexes(patterns []string) ([]string, error) {
	if len(patterns) <= 0 {
		return nil, nil
	}
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

		isOk := false
		for _, pattern := range patterns {
			isOk, err = regexp.Match(pattern, []byte(index))
			if err != nil {
				continue
			}

			if isOk {
				break
			}
		}

		if isOk {
			filteredIndexes = append(filteredIndexes, index)
		}
	}
	return filteredIndexes, nil
}

func (m *BulkMigrator) WithPatternIndexes(patterns []string) *BulkMigrator {
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
		ActionSize:        m.ActionSize,
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Patterns:          patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
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
		ActionSize:        m.ActionSize,
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
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
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
	}
}

func (m *BulkMigrator) WithQuery(query string) *BulkMigrator {
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
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             query,
		isCancelled:       m.isCancelled,
	}
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
		Ids:               m.Ids,
		ActionParallelism: m.ActionParallelism,
		IndexFilePairMap:  m.IndexFilePairMap,
		Patterns:          m.Patterns,
		IndexFileRoot:     m.IndexFileRoot,
		IndexTemplates:    m.IndexTemplates,
		taskProgress:      m.taskProgress,
		Query:             m.Query,
		isCancelled:       m.isCancelled,
	}
}

func (m *BulkMigrator) Sync(force bool) error {
	newBulkMigrator := m.getIndexPairsFromPattern()
	if newBulkMigrator.Error != nil {
		return errors.WithStack(newBulkMigrator.Error)
	}

	if err := newBulkMigrator.parallelRun(func(migrator *Migrator) error {
		if err := migrator.Sync(force); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *BulkMigrator) SyncDiff() (map[string]*DiffResult, error) {
	newBulkMigrator := m.getIndexPairsFromPattern()
	if newBulkMigrator.Error != nil {
		return nil, errors.WithStack(newBulkMigrator.Error)
	}

	var diffMap sync.Map
	err := newBulkMigrator.parallelRun(func(migrator *Migrator) error {
		diffResult, err := migrator.SyncDiff()
		if utils.IsCustomError(err, utils.NonIndexExisted) {
			diffMap.Store(newBulkMigrator.getIndexPairKey(migrator.IndexPair), &DiffResult{
				SameCount:   *utils.ZeroAtomicUint64(),
				CreateCount: *utils.MaxAtomicUint64(),
				UpdateCount: *utils.ZeroAtomicUint64(),
				DeleteCount: *utils.ZeroAtomicUint64(),
			})
		}

		if err != nil {
			return errors.WithStack(err)
		}
		if diffResult.HasDiff() {
			diffMap.Store(newBulkMigrator.getIndexPairKey(migrator.IndexPair), diffResult)
		}
		return nil
	})

	result := make(map[string]*DiffResult)
	diffMap.Range(func(key, value interface{}) bool {
		keyStr := cast.ToString(key)
		result[keyStr] = value.(*DiffResult)
		return true
	})

	return result, errors.WithStack(err)
}

func (m *BulkMigrator) Compare() (map[string]*DiffResult, error) {
	newBulkMigrator := m.getIndexPairsFromPattern()
	if newBulkMigrator.Error != nil {
		return nil, errors.WithStack(newBulkMigrator.Error)
	}

	var diffMap sync.Map

	err := newBulkMigrator.parallelRun(func(migrator *Migrator) error {
		diffResult, err := migrator.Compare()
		if utils.IsCustomError(err, utils.NonIndexExisted) {
			diffMap.Store(newBulkMigrator.getIndexPairKey(migrator.IndexPair), &DiffResult{
				SameCount:   *utils.ZeroAtomicUint64(),
				CreateCount: *utils.MaxAtomicUint64(),
				UpdateCount: *utils.ZeroAtomicUint64(),
				DeleteCount: *utils.ZeroAtomicUint64(),
			})
			return nil
		}

		if err != nil {
			return errors.WithStack(err)
		}
		if diffResult.HasDiff() {
			diffMap.Store(newBulkMigrator.getIndexPairKey(migrator.IndexPair), diffResult)
		}
		return nil
	})

	result := make(map[string]*DiffResult)

	diffMap.Range(func(key, value interface{}) bool {
		keyStr := cast.ToString(key)
		result[keyStr] = value.(*DiffResult)
		return true
	})

	return result, errors.WithStack(err)
}

func (m *BulkMigrator) CopyIndexSettings(force bool) error {
	newBulkMigrator := m.getIndexPairsFromPattern()
	if newBulkMigrator.Error != nil {
		return errors.WithStack(newBulkMigrator.Error)
	}

	if err := newBulkMigrator.parallelRun(func(migrator *Migrator) error {
		if err := migrator.CopyIndexSettings(force); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *BulkMigrator) CreateTemplates() error {
	if err := m.parallelRunWithIndexTemplate(func(migrator *Migrator) error {
		if err := migrator.CreateTemplate(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *BulkMigrator) getIndexPairsFromPattern() *BulkMigrator {
	if m.Error != nil {
		return m
	}

	var newBulkMigrator = m.clone()
	var filterIndexes []string

	filterIndexes, newBulkMigrator.Error = m.filterIndexes(m.Patterns)
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

func (m *BulkMigrator) getIndexFilePairsFromPattern() *BulkMigrator {
	if m.Error != nil {
		return m
	}

	var newBulkMigrator = m.clone()
	var filterIndexes []string

	filterIndexes, newBulkMigrator.Error = m.filterIndexes(m.Patterns)
	if newBulkMigrator.Error != nil {
		return newBulkMigrator
	}

	newIndexFilePairsMap := make(map[string]*config.IndexFilePair)
	for _, index := range filterIndexes {
		indexFilePair := &config.IndexFilePair{
			Index:        index,
			IndexFileDir: index,
		}

		if _, ok := newBulkMigrator.IndexPairMap[index]; !ok {
			newIndexFilePairsMap[index] = indexFilePair
		}
	}

	if len(newIndexFilePairsMap) > 0 {
		newBulkMigrator.IndexFilePairMap = lo.Assign(newBulkMigrator.IndexFilePairMap, newIndexFilePairsMap)
	}
	return newBulkMigrator
}

func (m *BulkMigrator) getIndexFilePairFromPattern() *BulkMigrator {
	if m.Error != nil {
		return m
	}

	var newBulkMigrator = m.clone()
	var filterIndexes []string

	filterIndexes, newBulkMigrator.Error = m.filterIndexes(m.Patterns)
	if newBulkMigrator.Error != nil {
		return newBulkMigrator
	}

	newIndexPairsMap := make(map[string]*config.IndexFilePair)
	for _, index := range filterIndexes {

		indexPair := &config.IndexFilePair{
			Index:        index,
			IndexFileDir: fmt.Sprintf("%s/%s", m.IndexFileRoot, index),
		}
		if _, ok := newBulkMigrator.IndexFilePairMap[index]; !ok {
			newIndexPairsMap[index] = indexPair
		}
	}

	if len(newIndexPairsMap) > 0 {
		newBulkMigrator.IndexFilePairMap = lo.Assign(newBulkMigrator.IndexFilePairMap, newIndexPairsMap)
	}
	return newBulkMigrator
}

func (m *BulkMigrator) getIndexFilePairFromIndexFileRoot() *BulkMigrator {
	if m.Error != nil {
		return m
	}

	if lo.IsEmpty(m.IndexFileRoot) {
		return m
	}

	var newBulkMigrator = m.clone()
	subDirectories, err := utils.GetSubDirectories(m.IndexFileRoot)
	if err != nil {
		newBulkMigrator.Error = errors.WithStack(err)
		return newBulkMigrator
	}

	newIndexFileMap := make(map[string]*config.IndexFilePair)

	for _, subDirectory := range subDirectories {
		settingFilePath := fmt.Sprintf("%s/setting.json", subDirectory)
		if !utils.FileIsExisted(settingFilePath) {
			continue
		}

		// read settingFile file content, marshal to map
		settingBytes, err := os.ReadFile(settingFilePath)
		if err != nil {
			newBulkMigrator.Error = errors.WithStack(err)
			return newBulkMigrator
		}

		settingFileMap := make(map[string]interface{})
		if err := json.Unmarshal(settingBytes, &settingFileMap); err != nil {
			newBulkMigrator.Error = errors.WithStack(err)
			return newBulkMigrator
		}

		index := cast.ToString(settingFileMap["index"])
		if len(m.Patterns) > 0 {
			isOk := false
			for _, pattern := range m.Patterns {
				isOk, err = regexp.Match(pattern, []byte(index))
				if err != nil {
					continue
				}

				if isOk {
					break
				}
			}

			if !isOk {
				continue
			}
		}

		if _, ok := m.IndexFilePairMap[index]; !ok {
			newIndexFileMap[index] = &config.IndexFilePair{
				Index:        index,
				IndexFileDir: subDirectory,
			}
		}
	}

	if len(newIndexFileMap) > 0 {
		newBulkMigrator.IndexFilePairMap = lo.Assign(newBulkMigrator.IndexFilePairMap, newIndexFileMap)
	}

	return newBulkMigrator
}

func (m *BulkMigrator) parallelRun(callback func(migrator *Migrator) error) error {
	pool := pond.New(cast.ToInt(m.Parallelism), len(m.IndexPairMap))
	m.taskProgress.Total = cast.ToUint64(len(m.IndexPairMap))
	var errs utils.Errs
	for _, indexPair := range m.IndexPairMap {
		if *m.isCancelled {
			break
		}
		newMigrator := NewMigrator(m.ctx, m.SourceES, m.TargetES, m.isCancelled)
		newMigrator = newMigrator.WithIndexPair(*indexPair).
			WithScrollSize(m.ScrollSize).
			WithScrollTime(m.ScrollTime).
			WithSliceSize(m.SliceSize).
			WithBufferCount(m.BufferCount).
			WithActionParallelism(m.ActionParallelism).
			WithActionSize(m.ActionSize).
			WithIds(m.Ids).
			WithQuery(m.Query)

		pool.Submit(func() {
			err := callback(newMigrator)
			if err != nil {
				newMigrator.sourceIndexPairProgress.Fail(newMigrator.GetCtx())
				m.taskProgress.FailCount.Add(1)
				errs.Add(err)
			} else {
				newMigrator.sourceIndexPairProgress.Finish(newMigrator.GetCtx())
				m.taskProgress.SuccessCount.Add(1)
			}
			m.taskProgress.Increment(1)
		})
	}
	pool.StopAndWait()
	return errs.Ret()
}

func (m *BulkMigrator) parallelRunWithIndexTemplate(callback func(migrator *Migrator) error) error {
	pool := pond.New(cast.ToInt(m.Parallelism), len(m.IndexPairMap))

	m.taskProgress.Total = cast.ToUint64(len(m.IndexPairMap))
	var errs utils.Errs
	for _, indexTemplate := range m.IndexTemplates {
		newMigrator := NewMigrator(m.ctx, m.SourceES, m.TargetES, m.isCancelled)
		newMigrator = newMigrator.WithIndexTemplate(*indexTemplate).
			WithScrollSize(m.ScrollSize).
			WithScrollTime(m.ScrollTime).
			WithSliceSize(m.SliceSize).
			WithBufferCount(m.BufferCount).
			WithActionParallelism(m.ActionParallelism).
			WithActionSize(m.ActionSize).
			WithIds(m.Ids).
			WithQuery(m.Query)

		pool.Submit(func() {
			err := callback(newMigrator)
			if err != nil {
				newMigrator.sourceIndexPairProgress.Fail(newMigrator.GetCtx())
				errs.Add(err)
				m.taskProgress.FailCount.Add(1)
			} else {
				newMigrator.sourceIndexPairProgress.Finish(newMigrator.GetCtx())
				m.taskProgress.SuccessCount.Add(1)
			}
			m.taskProgress.Increment(1)
		})
	}
	pool.StopAndWait()
	return errs.Ret()
}

func (m *BulkMigrator) parallelRunWithIndexFilePair(callback func(migrator *Migrator) error) error {
	pool := pond.New(cast.ToInt(m.Parallelism), len(m.IndexPairMap))

	m.taskProgress.Total = cast.ToUint64(len(m.IndexPairMap))
	var errs utils.Errs
	for _, indexFilePair := range m.IndexFilePairMap {
		newMigrator := NewMigrator(m.ctx, m.SourceES, m.TargetES, m.isCancelled)
		newMigrator = newMigrator.WithIndexFilePair(indexFilePair).
			WithScrollSize(m.ScrollSize).
			WithScrollTime(m.ScrollTime).
			WithSliceSize(m.SliceSize).
			WithBufferCount(m.BufferCount).
			WithActionParallelism(m.ActionParallelism).
			WithActionSize(m.ActionSize).
			WithIds(m.Ids).
			WithQuery(m.Query)

		pool.Submit(func() {
			err := callback(newMigrator)
			if err != nil {
				newMigrator.sourceIndexPairProgress.Fail(newMigrator.GetCtx())
				errs.Add(err)
				m.taskProgress.FailCount.Add(1)
			} else {
				newMigrator.sourceIndexPairProgress.Finish(newMigrator.GetCtx())
				m.taskProgress.SuccessCount.Add(1)
			}
			m.taskProgress.Increment(1)
		})
	}
	pool.StopAndWait()
	return errs.Ret()
}

func (m *BulkMigrator) Import(force bool) error {
	newBulkMigrator := m.getIndexFilePairFromIndexFileRoot()
	if newBulkMigrator.Error != nil {
		return errors.WithStack(newBulkMigrator.Error)
	}

	if err := newBulkMigrator.parallelRunWithIndexFilePair(func(migrator *Migrator) error {
		if err := migrator.Import(force); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *BulkMigrator) Export() error {
	newBulkMigrator := m.getIndexFilePairFromPattern()
	if newBulkMigrator.Error != nil {
		return errors.WithStack(newBulkMigrator.Error)
	}

	err := newBulkMigrator.parallelRunWithIndexFilePair(func(migrator *Migrator) error {
		if err := migrator.Export(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
