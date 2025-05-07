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
	isCancelled       *bool
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
		isCancelled:       lo.ToPtr(false),
	}, nil
}

func (t *TaskMgr) Run(ctx context.Context, taskNames ...string) error {
	ctx = utils.SetCtxKeyIgnoreSystemIndex(ctx, t.ignoreSystemIndex)

	for _, taskCfg := range t.taskCfgs {
		if len(taskNames) > 0 && !lo.Contains(taskNames, taskCfg.Name) {
			continue
		}
		task := NewTaskWithES(ctx, taskCfg, t.usedESMap[taskCfg.SourceES], t.usedESMap[taskCfg.TargetES], t.isCancelled)
		if err := task.Run(); err != nil {
			return errors.WithStack(err)
		}

		if *t.isCancelled {
			break
		}
	}

	return nil
}
