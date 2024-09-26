package service

import (
	"context"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/pkg/errors"
)

type TaskMgr struct {
	usedESMap         map[string]es.ES
	taskCfgs          []*config.TaskCfg
	showProgress      bool
	ignoreSystemIndex bool
}

func NewTaskMgr(cfg *config.Config) (*TaskMgr, error) {
	usedESMap := make(map[string]es.ES)
	for _, task := range cfg.Tasks {
		if cfg.ESConfigs[task.SourceES] == nil {
			return nil, fmt.Errorf("source es config not found: %s", task.SourceES)
		}

		if cfg.ESConfigs[task.TargetES] == nil {
			return nil, fmt.Errorf("target es config not found: %s", task.TargetES)
		}

		var err error
		sourceES := es.NewESV0(cfg.ESConfigs[task.SourceES])
		usedESMap[task.SourceES], err = sourceES.GetES()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		targetES := es.NewESV0(cfg.ESConfigs[task.TargetES])
		usedESMap[task.TargetES], err = targetES.GetES()
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	return &TaskMgr{
		usedESMap:         usedESMap,
		taskCfgs:          cfg.Tasks,
		showProgress:      cfg.ShowProgress,
		ignoreSystemIndex: cfg.IgnoreSystemIndex,
	}, nil
}

func (t *TaskMgr) Run(ctx context.Context) error {
	ctx = utils.SetCtxKeyShowProgress(ctx, t.showProgress)
	ctx = utils.SetCtxKeyIgnoreSystemIndex(ctx, t.ignoreSystemIndex)

	bar := utils.NewProgressBar(ctx, "All tasks", "", len(t.taskCfgs))

	for idx, taskCfg := range t.taskCfgs {
		task := NewTaskWithES(ctx, taskCfg, t.usedESMap[taskCfg.SourceES], t.usedESMap[taskCfg.TargetES])
		if err := task.Run(); err != nil {
			return errors.WithStack(err)
		}

		bar.Increment()
		utils.GetLogger(task.GetCtx()).Debug("task done")
		utils.GetLogger(task.GetCtx()).Infof("tasks progress %0.4f (%d, %d)", float64(idx+1)/float64(len(t.taskCfgs)), idx+1, len(t.taskCfgs))
	}

	bar.Finish()
	return nil
}
