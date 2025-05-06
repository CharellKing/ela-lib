package task

import (
	"context"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"strings"
)

type Task struct {
	bulkMigrator *BulkMigrator
	force        bool
	showProgress bool
	isCancelled  *bool
}

func NewTaskWithES(ctx context.Context, taskCfg *config.TaskCfg, sourceES, targetES es.ES, isCancelled *bool) *Task {
	taskId := uuid.New().String()

	if lo.IsNotEmpty(sourceES) {
		ctx = utils.SetCtxKeySourceESVersion(ctx, sourceES.GetClusterVersion())
	}

	if lo.IsNotEmpty(targetES) {
		ctx = utils.SetCtxKeyTargetESVersion(ctx, targetES.GetClusterVersion())
	}

	ctx = utils.SetCtxKeyTaskName(ctx, taskCfg.Name)
	ctx = utils.SetCtxKeyTaskID(ctx, taskId)
	ctx = utils.SetCtxKeyTaskAction(ctx, string(taskCfg.TaskAction))

	bulkMigrator := NewBulkMigratorWithES(ctx, sourceES, targetES, isCancelled)
	bulkMigrator = bulkMigrator.WithIndexPairs(taskCfg.IndexPairs...).
		WithParallelism(taskCfg.Parallelism).
		WithScrollSize(taskCfg.ScrollSize).
		WithScrollTime(taskCfg.ScrollTime).
		WithSliceSize(taskCfg.SliceSize).
		WithBufferCount(taskCfg.BufferCount).
		WithActionParallelism(taskCfg.ActionParallelism).
		WithActionSize(taskCfg.ActionSize).
		WithIds(taskCfg.Ids).
		WithIndexFilePairs(taskCfg.IndexFilePairs...).
		WithIndexFileRoot(taskCfg.IndexFileRoot).
		WithIndexTemplates(taskCfg.IndexTemplates...).
		WithQuery(taskCfg.Query)
	if taskCfg.IndexPattern != nil {
		bulkMigrator = bulkMigrator.WithPatternIndexes(*taskCfg.IndexPattern)
	}

	return &Task{
		bulkMigrator: bulkMigrator,
		force:        taskCfg.Force,
		isCancelled:  isCancelled,
	}
}

func NewTask(ctx context.Context, taskCfg *config.TaskCfg, cfg *config.Config) (*Task, error) {
	if cfg == nil {
		return nil, nil

	}

	sourceESV0 := es.NewESV0(cfg.ESConfigs[taskCfg.SourceES])
	sourceES, err := sourceESV0.GetES()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	targetESV0 := es.NewESV0(cfg.ESConfigs[taskCfg.TargetES])
	targetES, err := targetESV0.GetES()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return NewTaskWithES(ctx, taskCfg, sourceES, targetES, lo.ToPtr(false)), nil
}

func (t *Task) GetCtx() context.Context {
	return t.bulkMigrator.GetCtx()
}

func (t *Task) Compare() (map[string]*DiffResult, error) {
	return t.bulkMigrator.Compare()
}

func (t *Task) SyncDiff() (map[string]*DiffResult, error) {
	return t.bulkMigrator.SyncDiff()
}

func (t *Task) Sync() error {
	return t.bulkMigrator.Sync(t.force)
}

func (t *Task) CopyIndexSettings() error {
	return t.bulkMigrator.CopyIndexSettings(t.force)
}

func (t *Task) Import() error {
	return t.bulkMigrator.Import(t.force)
}

func (t *Task) Export() error {
	return t.bulkMigrator.Export()
}

func (t *Task) CreateTemplate() error {
	return t.bulkMigrator.CreateTemplates()
}

func (t *Task) Run() error {
	ctx := t.GetCtx()
	taskAction := config.TaskAction(utils.GetCtxKeyTaskAction(ctx))
	switch taskAction {
	case config.TaskActionCopyIndex:
		return t.CopyIndexSettings()
	case config.TaskActionSync:
		return t.Sync()
	case config.TaskActionSyncDiff:
		diffResultMap, err := t.SyncDiff()
		if err != nil {
			return errors.WithStack(err)
		}

		for indexes, diffResult := range diffResultMap {
			indexArray := strings.Split(indexes, ":")
			utils.GetTaskLogger(t.GetCtx()).
				WithField("sourceIndex", indexArray[0]).
				WithField("targetIndex", indexArray[1]).
				WithField("percent", diffResult.Percent()).
				WithField("create", diffResult.CreateCount.Load()).
				WithField("update", diffResult.UpdateCount.Load()).
				WithField("delete", diffResult.DeleteCount.Load()).
				WithField("createDocs", diffResult.CreateDocs).
				WithField("updateDocs", diffResult.UpdateDocs).
				WithField("deleteDocs", diffResult.DeleteDocs).
				Info("difference")
		}
	case config.TaskActionCompare:
		diffResultMap, err := t.bulkMigrator.Compare()
		if err != nil {
			return errors.WithStack(err)
		}

		for indexes, diffResult := range diffResultMap {
			indexArray := strings.Split(indexes, ":")
			utils.GetTaskLogger(t.GetCtx()).
				WithField("sourceIndex", indexArray[0]).
				WithField("targetIndex", indexArray[1]).
				WithField("percent", diffResult.Percent()).
				WithField("create", diffResult.CreateCount.Load()).
				WithField("update", diffResult.UpdateCount.Load()).
				WithField("delete", diffResult.DeleteCount.Load()).
				WithField("createDocs", diffResult.CreateDocs).
				WithField("updateDocs", diffResult.UpdateDocs).
				WithField("deleteDocs", diffResult.DeleteDocs).
				Info("difference")
		}
	case config.TaskActionImport:
		return t.Import()
	case config.TaskActionExport:
		return t.Export()
	case config.TaskActionTemplate:
		return t.CreateTemplate()
	default:
		taskName := utils.GetCtxKeyTaskName(ctx)
		return fmt.Errorf("%s invalid task action %s", taskName, taskAction)
	}
	return nil
}
