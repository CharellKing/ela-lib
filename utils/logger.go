package utils

import (
	"context"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"os"
)

var logger *log.Logger

type progressCallBackType func(ctx context.Context)

var progressCallBack progressCallBackType

func InitLogger(cfg *config.Config, callback progressCallBackType) {
	levelMap := map[string]log.Level{
		"debug": log.DebugLevel,
		"info":  log.InfoLevel,
		"warn":  log.WarnLevel,
		"error": log.ErrorLevel,
	}

	level, ok := levelMap[cfg.Level]
	if !ok {
		level = log.InfoLevel
	}
	logger = &log.Logger{
		Out:       os.Stdout,
		Formatter: &log.JSONFormatter{},
		Hooks:     make(log.LevelHooks),
		Level:     level,
	}
	logger.SetReportCaller(true)

	progressCallBack = callback
}

func GetTaskLoggerProgress(ctx context.Context, sourceProgress *Progress, targetProgress *Progress) *log.Entry {
	entry := GetTaskLogger(ctx)

	if sourceProgress != nil {
		entry = entry.WithField("sourceProgress", fmt.Sprintf("%d/%d", sourceProgress.Current, sourceProgress.Total))
	}

	if targetProgress != nil {
		entry = entry.WithField("targetProgress", fmt.Sprintf("%d/%d", targetProgress.Current, targetProgress.Total))
	}

	GoRecovery(ctx, func() {
		if progressCallBack != nil {
			progressCallBack(ctx)
		}
	})

	return entry
}

func GetTaskLogger(ctx context.Context) *log.Entry {

	entry := log.NewEntry(logger)

	taskProgress := GetCtxKeyTaskProgress(ctx)
	if taskProgress != nil {
		entry = entry.WithField("taskProgress", fmt.Sprintf("%d/%d", taskProgress.Current.Load(), taskProgress.Total))
	}

	ctxKeyMap := map[CtxKey]func(ctx context.Context) string{
		CtxKeySourceESVersion: GetCtxKeySourceESVersion,
		CtxKeyTargetESVersion: GetCtxKeyTargetESVersion,
		CtxKeySourceObject:    GetCtxKeySourceObject,
		CtxKeyTargetObject:    GetCtxKeyTargetObject,
		CtxKeyTaskName:        GetCtxKeyTaskName,
		CtxKeyTaskID:          GetCtxKeyTaskID,
		CtxKeyTaskAction:      GetCtxKeyTaskAction,
	}
	for key, ctxFunc := range ctxKeyMap {
		value := ctx.Value(key)
		if lo.IsNotEmpty(value) {
			entry = entry.WithField(string(key), ctxFunc(ctx))
		}
	}
	return entry
}

func GetLogger(ctx context.Context) *log.Entry {
	return log.NewEntry(logger)
}
