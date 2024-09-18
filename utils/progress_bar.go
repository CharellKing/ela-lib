package utils

import (
	"context"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"github.com/samber/lo"
)

type ProgressBar struct {
	*pb.ProgressBar
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

	var bar *pb.ProgressBar
	if showProgress {
		tmpl := `{{ red "%s:" }} {{ bar . "<" "-" (cycle . "↖" "↗" "↘" "↙" ) "." ">"}} {{speed . | rndcolor }} {{percent .}} {{string . "my_green_string" | green}} {{string . "my_blue_string" | blue}}\n`

		bar = pb.ProgressBarTemplate(fmt.Sprintf(tmpl, title)).Start(total)
		bar = bar.Set("my_green_string", "green").Set("my_blue_string", "blue")
	}

	return &ProgressBar{bar, showProgress}
}

func (bar *ProgressBar) Increment() *ProgressBar {
	if !bar.showProgress {
		return bar
	}
	bar.ProgressBar = bar.Add(1)
	return bar
}

func (bar *ProgressBar) Finish() *ProgressBar {
	if !bar.showProgress {
		return bar
	}

	bar.ProgressBar = bar.ProgressBar.Finish()
	return bar
}
