package utils

import (
	"context"
	"github.com/cheggaaa/pb/v3"
	"github.com/samber/lo"
)

type ProgressBar struct {
	bar          *pb.ProgressBar
	title        string
	showProgress bool
}

func NewProgressBar(ctx context.Context, titlePrefix string, titleSuffix string, total int) *ProgressBar {
	showProgress := GetCtxKeyShowProgress(ctx)
	taskName := GetCtxKeyTaskName(ctx)
	sourceIndex := GetCtxKeySourceIndex(ctx)
	targetIndex := GetCtxKeyTargetIndex(ctx)
	action := GetCtxKeyTaskAction(ctx)

	title := titlePrefix
	if !lo.IsEmpty(taskName) {
		title += "." + taskName
	}

	if !lo.IsEmpty(action) {
		title += "." + action
	}

	if !lo.IsEmpty(sourceIndex) && !lo.IsEmpty(targetIndex) {
		title += "." + sourceIndex + "->" + targetIndex
	}

	if !lo.IsEmpty(titleSuffix) {
		title += "." + titleSuffix
	}

	if showProgress {
		bar := pb.New(total).Set("prefix", title)
		bar.Start()
		return &ProgressBar{bar: bar, title: title, showProgress: showProgress}
	}

	return &ProgressBar{showProgress: showProgress, title: title}
}

func (bar *ProgressBar) Increment() {
	if !bar.showProgress {
		return
	}
	bar.bar.Increment()
}

func (bar *ProgressBar) Finish() {
	if !bar.showProgress {
		return
	}
	bar.bar.Finish()
}
