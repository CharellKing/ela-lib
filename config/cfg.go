package config

type TaskAction string

const (
	TaskActionCopyIndex TaskAction = "copy_index"
	TaskActionSync      TaskAction = "sync"
	TaskActionSyncDiff  TaskAction = "sync_diff"
	TaskActionCompare   TaskAction = "compare"
	TaskActionImport    TaskAction = "import"
	TaskActionExport    TaskAction = "export"
)

type TaskCfg struct {
	Name               string           `mapstructure:"name"`
	IndexPattern       *string          `mapstructure:"index_pattern"`
	SourceES           string           `mapstructure:"source_es"`
	TargetES           string           `mapstructure:"target_es"`
	IndexPairs         []*IndexPair     `mapstructure:"index_pairs"`
	TaskAction         TaskAction       `mapstructure:"action"`
	Force              bool             `mapstructure:"force"`
	ScrollSize         uint             `mapstructure:"scroll_size"`
	ScrollTime         uint             `mapstructure:"scroll_time"`
	Parallelism        uint             `mapstructure:"parallelism"`
	SliceSize          uint             `mapstructure:"slice_size"`
	BufferCount        uint             `mapstructure:"buffer_count"`
	WriteParallelism   uint             `mapstructure:"write_parallelism"`
	WriteSize          uint             `mapstructure:"write_size"`
	Ids                []string         `mapstructure:"ids"`
	CompareParallelism uint             `mapstructure:"compare_parallelism"`
	IndexFilePairs     []*IndexFilePair `mapstructure:"index_file_pairs"`
	IndexFileDir       string           `mapstructure:"index_file_dir"`
}

type IndexPair struct {
	SourceIndex string `mapstructure:"source_index"`
	TargetIndex string `mapstructure:"target_index"`
}

type IndexFilePair struct {
	Index string `mapstructure:"index"`
	File  string `mapstructure:"file"`
}

type ESConfig struct {
	Addresses []string `mapstructure:"addresses"`
	User      string   `mapstructure:"user"`
	Password  string   `mapstructure:"password"`
}

type Config struct {
	ESConfigs         map[string]*ESConfig `mapstructure:"elastics"`
	Tasks             []*TaskCfg           `mapstructure:"tasks"`
	Level             string               `mapstructure:"level"`
	ShowProgress      bool                 `mapstructure:"show_progress"`
	IgnoreSystemIndex bool                 `mapstructure:"ignore_system_index"`
}
