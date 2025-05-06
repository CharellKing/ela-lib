package task

import (
	"context"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/spf13/viper"
	"testing"
)

func TestRun(t *testing.T) {
	configPath := "D:\\code\\ela-lib\\config.yaml"
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Unable reading config file, %v\n", err)
		return
	}

	var cfg config.Config
	if err := viper.Unmarshal(&cfg); err != nil {
		fmt.Printf("Unable to decode into struct, %v\n", err)
		return
	}

	utils.InitLogger(&cfg, nil)

	ctx := context.Background()
	taskMgr, err := NewTaskMgr(&cfg)
	if err != nil {
		utils.GetTaskLogger(ctx).Errorf("create task manager %+v", err)
		return
	}
	if err := taskMgr.Run(ctx); err != nil {
		utils.GetTaskLogger(ctx).Errorf("run task manager %+v", err)
		return
	}
}
