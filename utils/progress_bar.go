package utils

import (
	"context"
	"fmt"
	"github.com/gosuri/uiprogress"
	"github.com/samber/lo"
)

type ProgressBar struct {
	bar          *uiprogress.Bar
	progress     *uiprogress.Progress
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
		uip := uiprogress.New()
		uip.Start()
		bar := uip.AddBar(total).AppendCompleted().PrependElapsed()
		return &ProgressBar{bar: bar, progress: uip, title: title, showProgress: showProgress}
	}

	return &ProgressBar{showProgress: showProgress, title: title}
}

func (bar *ProgressBar) Step(stepTitle string) {
	if !bar.showProgress {
		return
	}
	bar.bar.PrependFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("%s: %d / %d [%s]", bar.title, bar.bar.Current(), bar.bar.Total, stepTitle)
	})
}

func (bar *ProgressBar) Increment() {
	if !bar.showProgress {
		return
	}
	bar.bar.Incr()
}

func (bar *ProgressBar) Finish() {
	if !bar.showProgress {
		return
	}

	bar.Step("done")
	bar.progress.Stop()
}
