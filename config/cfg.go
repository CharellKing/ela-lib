package config

type TaskAction string

const (
	TaskActionCopyIndex TaskAction = "copy_index"
	TaskActionSync      TaskAction = "sync"
	TaskActionSyncDiff  TaskAction = "sync_diff"
	TaskActionCompare   TaskAction = "compare"
	TaskActionImport    TaskAction = "import"
	TaskActionExport    TaskAction = "export"
	TaskActionTemplate  TaskAction = "create_template"
)

type TaskCfg struct {
	Name              string           `mapstructure:"name"`
	IndexPattern      *string          `mapstructure:"index_pattern"`
	SourceES          string           `mapstructure:"source_es"`
	TargetES          string           `mapstructure:"target_es"`
	IndexPairs        []*IndexPair     `mapstructure:"index_pairs"`
	IndexTemplates    []*IndexTemplate `mapstructure:"index_templates"`
	TaskAction        TaskAction       `mapstructure:"action"`
	Force             bool             `mapstructure:"force"`
	ScrollSize        uint             `mapstructure:"scroll_size"`
	ScrollTime        uint             `mapstructure:"scroll_time"`
	Parallelism       uint             `mapstructure:"parallelism"`
	SliceSize         uint             `mapstructure:"slice_size"`
	BufferCount       uint             `mapstructure:"buffer_count"`
	ActionParallelism uint             `mapstructure:"action_parallelism"`
	ActionSize        uint             `mapstructure:"action_size"`
	Ids               []string         `mapstructure:"ids"`
	IndexFilePairs    []*IndexFilePair `mapstructure:"index_file_pairs"`
	IndexFileRoot     string           `mapstructure:"index_file_root"`
}

type IndexPair struct {
	SourceIndex string `mapstructure:"source_index"`
	TargetIndex string `mapstructure:"target_index"`
}

type IndexFilePair struct {
	Index        string `mapstructure:"index"`
	IndexFileDir string `mapstructure:"index_file_dir"`
}

type IndexTemplate struct {
	Name     string   `mapstructure:"name"`
	Patterns []string `mapstructure:"pattern"`
	Order    int      `mapstructure:"order"`
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
	IgnoreSystemIndex bool                 `mapstructure:"ignore_system_index"`
	GatewayCfg        *GatewayCfg          `mapstructure:"gateway"`
}

type GatewayCfg struct {
	Address  string `mapstructure:"gateway_address"`
	User     string `mapstructure:"gateway_user"`
	Password string `mapstructure:"gateway_password"`

	SourceES string `mapstructure:"source_es"`
	TargetES string `mapstructure:"target_es"`
	Master   string `mapstructure:"master"`
}
