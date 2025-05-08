package utils

import (
	"context"
	"github.com/spf13/cast"
)

type CtxKey string

const (
	CtxKeySourceESVersion CtxKey = "sourceEsVersion"
	CtxKeyTargetESVersion CtxKey = "targetEsVersion"
	CtxKeySourceObject    CtxKey = "sourceObject"
	CtxKeyTargetObject    CtxKey = "targetObject"
	CtxKeyTaskName        CtxKey = "taskName"
	CtxKeyTaskID          CtxKey = "taskId"
	CtxKeyTaskAction      CtxKey = "taskAction"

	CtxKeySourceIndexSetting CtxKey = "sourceIndexSetting"
	CtxKeyTargetIndexSetting CtxKey = "targetIndexSetting"

	CtxKeySourceFieldMap CtxKey = "sourceFieldMap"
	CtxKeyTargetFieldMap CtxKey = "targetFieldMap"

	CtxKeyDateTimeFormatFixFields CtxKey = "dateTimeFormatFixFields"

	CtxKeyIgnoreSystemIndex CtxKey = "ignoreSystemIndex"

	CtxKeyTaskProgress                CtxKey = "taskProgress"
	CtxKeyTaskSourceIndexPairProgress CtxKey = "taskSourceIndexPairProgress"
	CtxKeyTargetIndexPairProgress     CtxKey = "targetIndexPairProgress"

	CtxKeySourceQueueExtrusion CtxKey = "sourceQueueExtrusion"
	CtxKeyTargetQueueExtrusion CtxKey = "targetQueueExtrusion"

	CtxKeyPairProgress CtxKey = "pairProgress"
)

func GetCtxKeySourceESVersion(ctx context.Context) string {
	return cast.ToString(ctx.Value(CtxKeySourceESVersion))
}

func GetCtxKeyTargetESVersion(ctx context.Context) string {
	return cast.ToString(ctx.Value(CtxKeyTargetESVersion))
}

func SetCtxKeySourceESVersion(ctx context.Context, version string) context.Context {
	return context.WithValue(ctx, CtxKeySourceESVersion, version)
}

func SetCtxKeyTargetESVersion(ctx context.Context, version string) context.Context {
	return context.WithValue(ctx, CtxKeyTargetESVersion, version)
}

func GetCtxKeySourceObject(ctx context.Context) string {
	return cast.ToString(ctx.Value(CtxKeySourceObject))
}

func GetCtxKeyTargetObject(ctx context.Context) string {
	return cast.ToString(ctx.Value(CtxKeyTargetObject))
}

func SetCtxKeySourceObject(ctx context.Context, obj string) context.Context {
	return context.WithValue(ctx, CtxKeySourceObject, obj)
}

func SetCtxKeyTargetObject(ctx context.Context, index string) context.Context {
	return context.WithValue(ctx, CtxKeyTargetObject, index)
}

func GetCtxKeyTaskName(ctx context.Context) string {
	return cast.ToString(ctx.Value(CtxKeyTaskName))
}

func GetCtxKeyTaskID(ctx context.Context) string {
	return cast.ToString(ctx.Value(CtxKeyTaskID))
}

func SetCtxKeyTaskName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, CtxKeyTaskName, name)
}

func SetCtxKeyTaskID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, CtxKeyTaskID, id)
}

func GetCtxKeyTaskAction(ctx context.Context) string {
	return cast.ToString(ctx.Value(CtxKeyTaskAction))
}

func SetCtxKeyTaskAction(ctx context.Context, action string) context.Context {
	return context.WithValue(ctx, CtxKeyTaskAction, action)
}

func GetCtxKeySourceIndexSetting(ctx context.Context) interface{} {
	return ctx.Value(CtxKeySourceIndexSetting)
}

func SetCtxKeySourceIndexSetting(ctx context.Context, setting interface{}) context.Context {
	return context.WithValue(ctx, CtxKeySourceIndexSetting, setting)
}

func GetCtxKeyTargetIndexSetting(ctx context.Context) interface{} {
	return ctx.Value(CtxKeyTargetIndexSetting)
}

func SetCtxKeyTargetIndexSetting(ctx context.Context, setting interface{}) context.Context {
	return context.WithValue(ctx, CtxKeyTargetIndexSetting, setting)
}

func GetCtxKeySourceFieldMap(ctx context.Context) map[string]interface{} {
	return ctx.Value(CtxKeySourceFieldMap).(map[string]interface{})
}

func SetCtxKeySourceFieldMap(ctx context.Context, fieldMap map[string]interface{}) context.Context {
	return context.WithValue(ctx, CtxKeySourceFieldMap, fieldMap)
}

func GetCtxKeyTargetFieldMap(ctx context.Context) map[string]interface{} {
	return ctx.Value(CtxKeyTargetFieldMap).(map[string]interface{})
}

func SetCtxKeyTargetFieldMap(ctx context.Context, fieldMap map[string]interface{}) context.Context {
	return context.WithValue(ctx, CtxKeyTargetFieldMap, fieldMap)
}

func GetCtxKeyDateTimeFormatFixFields(ctx context.Context) map[string]string {
	return ctx.Value(CtxKeyDateTimeFormatFixFields).(map[string]string)
}

func SetCtxKeyDateTimeFormatFixFields(ctx context.Context, fields map[string]string) context.Context {
	return context.WithValue(ctx, CtxKeyDateTimeFormatFixFields, fields)
}

func GetCtxKeyIgnoreSystemIndex(ctx context.Context) bool {
	return cast.ToBool(ctx.Value(CtxKeyIgnoreSystemIndex))
}

func SetCtxKeyIgnoreSystemIndex(ctx context.Context, ignoreSystemIndex bool) context.Context {
	return context.WithValue(ctx, CtxKeyIgnoreSystemIndex, ignoreSystemIndex)
}

func GetCtxKeyTaskProgress(ctx context.Context) *Progress {
	taskProgress := ctx.Value(CtxKeyTaskProgress)
	if taskProgress == nil {
		return nil
	}
	return taskProgress.(*Progress)
}

func SetCtxKeyTaskProgress(ctx context.Context, taskProgress *Progress) context.Context {
	return context.WithValue(ctx, CtxKeyTaskProgress, taskProgress)
}

func GetCtxKeySourceIndexPairProgress(ctx context.Context) *Progress {
	indexPairProgress := ctx.Value(CtxKeyTaskSourceIndexPairProgress)
	if indexPairProgress == nil {
		return nil
	}
	return indexPairProgress.(*Progress)
}

func SetCtxKeySourceIndexPairProgress(ctx context.Context, indexPairProgress *Progress) context.Context {
	return context.WithValue(ctx, CtxKeyTaskSourceIndexPairProgress, indexPairProgress)
}

func GetCtxKeyTargetIndexPairProgress(ctx context.Context) *Progress {
	indexPairProgress := ctx.Value(CtxKeyTargetIndexPairProgress)
	if indexPairProgress == nil {
		return nil
	}
	return indexPairProgress.(*Progress)
}

func SetCtxKeyTargetIndexPairProgress(ctx context.Context, indexPairProgress *Progress) context.Context {
	return context.WithValue(ctx, CtxKeyTargetIndexPairProgress, indexPairProgress)
}

func GetCtxKeySourceQueueExtrusion(ctx context.Context) *Progress {
	queueExtrusion := ctx.Value(CtxKeySourceQueueExtrusion)
	if queueExtrusion == nil {
		return nil
	}
	return queueExtrusion.(*Progress)
}

func SetCtxKeySourceQueueExtrusion(ctx context.Context, queueExtrusion *Progress) context.Context {
	return context.WithValue(ctx, CtxKeySourceQueueExtrusion, queueExtrusion)
}

func GetCtxKeyTargetQueueExtrusion(ctx context.Context) *Progress {
	queueExtrusion := ctx.Value(CtxKeyTargetQueueExtrusion)
	if queueExtrusion == nil {
		return nil
	}
	return queueExtrusion.(*Progress)
}

func SetCtxKeyTargetQueueExtrusion(ctx context.Context, queueExtrusion *Progress) context.Context {
	return context.WithValue(ctx, CtxKeyTargetQueueExtrusion, queueExtrusion)
}

func GetCtxKeyPairProgress(ctx context.Context) *Progress {
	pairProgress := ctx.Value(CtxKeyPairProgress)
	if pairProgress == nil {
		return nil
	}
	return pairProgress.(*Progress)
}

func SetCtxKeyPairProgress(ctx context.Context, pairProgress *Progress) context.Context {
	return context.WithValue(ctx, CtxKeyPairProgress, pairProgress)
}
