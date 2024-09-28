package es

import (
	"context"
	"fmt"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/hashicorp/go-version"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"strings"
)

type fixCallback func(ctx context.Context, doc *Doc) *Doc

type FixUnit struct {
	SourceESVersionRange *version.Constraints
	TargetESVersionRange *version.Constraints
	Actions              []string
	Callback             fixCallback
}

func (f *FixUnit) IsMatch(sourceESVersion, targetESVersion *version.Version) bool {
	isOk := true
	if f.SourceESVersionRange != nil {
		isOk = f.SourceESVersionRange.Check(sourceESVersion)
	}

	if isOk && f.TargetESVersionRange != nil {
		isOk = f.TargetESVersionRange.Check(targetESVersion)
	}
	return isOk
}

var fixUnits []FixUnit

func init() {
	version5, _ := version.NewConstraint(">= 5.0, < 6.0")
	nonVersion5, _ := version.NewConstraint(">= 6.0")
	fixUnits = []FixUnit{
		{
			SourceESVersionRange: &version5,
			TargetESVersionRange: &nonVersion5,
			Actions:              []string{"import", "sync", "sync_diff", "compare"},
			Callback:             fixDatetimeFormatDate,
		},
	}
}

func getVersion(esVersionStr string) (*version.Version, error) {
	if lo.IsEmpty(esVersionStr) {
		return nil, nil
	}
	return version.NewVersion(esVersionStr)
}

func FixDoc(ctx context.Context, doc *Doc) (*Doc, error) {
	sourceESVersionStr := utils.GetCtxKeySourceESVersion(ctx)
	targetESVersionStr := utils.GetCtxKeyTargetESVersion(ctx)

	if sourceESVersionStr == targetESVersionStr {
		return doc, nil
	}

	sourceESVersion, err := getVersion(sourceESVersionStr)
	if err != nil {
		return doc, errors.WithStack(err)
	}

	targetESVersion, err := getVersion(targetESVersionStr)
	if err != nil {
		return doc, errors.WithStack(err)
	}

	for _, fixUnit := range fixUnits {
		if !lo.Contains(fixUnit.Actions, utils.GetCtxKeyTaskAction(ctx)) {
			continue
		}
		if fixUnit.IsMatch(sourceESVersion, targetESVersion) {
			doc = fixUnit.Callback(ctx, doc)
		}
	}
	return doc, nil
}

func fixDatetimeFormatDate(ctx context.Context, doc *Doc) *Doc {
	datetimeFields := utils.GetCtxKeyDateTimeFormatFixFields(ctx)
	newSource := make(map[string]interface{})
	for fieldName, fieldValue := range doc.Source {
		newSource[fieldName] = fieldValue
		if datetimeFieldFormat, ok := datetimeFields[fieldName]; ok {
			fieldValueStr := cast.ToString(fieldValue)
			valueSections := strings.Split(fieldValueStr, ":")
			formatSections := strings.Split(datetimeFieldFormat, ":")

			format := fmt.Sprintf("%%0%dd", len(formatSections[3]))
			if len(valueSections) == 3 {
				valueSections = append(valueSections, fmt.Sprintf(format, 0))
			} else if len(valueSections) > 3 {
				secondFraction := cast.ToInt(strings.TrimLeft(valueSections[3], "0"))
				valueSections[3] = fmt.Sprintf(format, secondFraction)
			}
			newSource[fieldName] = strings.Join(valueSections, ":")
		}
	}
	doc.Source = newSource
	return doc
}
