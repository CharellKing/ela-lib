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

func InitLogger(cfg *config.Config) {
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
