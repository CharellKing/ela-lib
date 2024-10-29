package task

import (
	"context"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

type TaskMgr struct {
	usedESMap         map[string]es.ES
	taskCfgs          []*config.TaskCfg
	ignoreSystemIndex bool
}

func NewTaskMgr(cfg *config.Config) (*TaskMgr, error) {
	usedESMap := make(map[string]es.ES)
	for _, task := range cfg.Tasks {
		for _, esCfgName := range []string{task.SourceES, task.TargetES} {
			if lo.IsEmpty(esCfgName) {
				continue
			}

			if cfg.ESConfigs[esCfgName] == nil {
				return nil, fmt.Errorf("es config not found: %s", esCfgName)
			}

			var err error
			esInstance := es.NewESV0(cfg.ESConfigs[esCfgName])
			usedESMap[esCfgName], err = esInstance.GetES()
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}

	return &TaskMgr{
		usedESMap:         usedESMap,
		taskCfgs:          cfg.Tasks,
		ignoreSystemIndex: cfg.IgnoreSystemIndex,
	}, nil
}

func (t *TaskMgr) Run(ctx context.Context, taskNames ...string) error {
	ctx = utils.SetCtxKeyIgnoreSystemIndex(ctx, t.ignoreSystemIndex)

	for idx, taskCfg := range t.taskCfgs {
		if len(taskNames) > 0 && !lo.Contains(taskNames, taskCfg.Name) {
			continue
		}
		task := NewTaskWithES(ctx, taskCfg, t.usedESMap[taskCfg.SourceES], t.usedESMap[taskCfg.TargetES])
		if err := task.Run(); err != nil {
			return errors.WithStack(err)
		}

		utils.GetLogger(task.GetCtx()).Debug("task done")
		utils.GetLogger(task.GetCtx()).Infof("tasks progress %0.4f (%d, %d)", float64(idx+1)/float64(len(t.taskCfgs)), idx+1, len(t.taskCfgs))
	}

	return nil
}
