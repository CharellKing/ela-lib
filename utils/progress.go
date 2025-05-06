package utils

import "sync/atomic"

type ProgressType string

const (
	ProgressNameTask                    = "task-progress"
	ProgressNameCompareSourceIndexPair  = "compare-source-index-pair-progress"
	ProgressNameCompareTargetIndexPair  = "compare-target-index-pair-progress"
	ExtrusionNameCompareSourceIndexPair = "compare-source-index-pair-extrusion"
	ExtrusionNameCompareTargetIndexPair = "compare-target-index-pair-extrusion"
	ProgressNameDeleteSourceIndexPair   = "delete-source-index-pair-progress"
	ExtrusionNameDeleteTargetIndexPair  = "delete-target-index-pair-extrusion"
	ProgressNameUpsertSourceIndexPair   = "upsert-source-index-pair-progress"
	ExtrusionNameUpsertTargetIndexPair  = "upsert-target-index-pair-extrusion"
	ProgressNameImportSourceIndexPair   = "import-source-index-pair-progress"
	ExtrusionNameImportSourceIndexPair  = "import-source-index-pair-extrusion"
	ProgressNameExportSourceIndexPair   = "export-source-index-pair-progress"
	ExtrusionNameExportSourceIndexPair  = "export-source-index-pair-extrusion"
)
const (
	ProgressTypeProgress       ProgressType = "progress"
	ProgressTypeQueueExtrusion ProgressType = "extrusion"
)

type Progress struct {
	Name    string
	Type    ProgressType
	Total   uint64
	Current atomic.Uint64
}

func NewProgress(name string, total uint64) *Progress {
	return &Progress{
		Name:    name,
		Type:    ProgressTypeProgress,
		Total:   total,
		Current: atomic.Uint64{},
	}
}

func NewExtrusion(name string, total uint64) *Progress {
	return &Progress{
		Name:    name,
		Type:    ProgressTypeQueueExtrusion,
		Total:   total,
		Current: atomic.Uint64{},
	}
}

func (p *Progress) Clear() {
	p.Name = ""
	p.Total = 0
	p.Current.Store(0)
}

func (p *Progress) Reset(name string, total uint64) {
	p.Name = name
	p.Total = total
	p.Current.Store(0)
}

func (p *Progress) Increment(delta uint64) {
	p.Current.Add(delta)
}

func (p *Progress) Set(current uint64) {
	p.Current.Store(current)
}

func (p *Progress) Finish() {
	p.Current.Store(p.Total)
}
