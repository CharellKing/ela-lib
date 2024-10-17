package gateway

import (
	"context"
	"encoding/json"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/pkg/es"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/spf13/viper"
	"testing"
)

func TestClusterHealth(t *testing.T) {
	configPath := "D:\\code\\ela-lib\\config.yaml"
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		t.Error(err)
		return
	}

	var cfg config.Config
	if err := viper.Unmarshal(&cfg); err != nil {
		t.Error(err)
		return
	}

	utils.InitLogger(&cfg)
	ctx := context.Background()
	v0 := es.NewESV0(cfg.ESConfigs["es5"])
	esInstance, err := v0.GetES()
	if err != nil {
		t.Error(err)
		return
	}
	resp, err := esInstance.ClusterHealth(ctx)
	if err != nil {
		t.Error(err)
		return
	}

	respStr, _ := json.MarshalIndent(resp, "", "  ")
	t.Log(string(respStr))
}
