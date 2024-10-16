package gateway

import (
	"context"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/spf13/viper"
	"testing"
)

func TestGatewayServer(t *testing.T) {
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

	utils.InitLogger(&cfg)
	ctx := context.Background()
	esProxy, err := NewESGateway(&cfg)
	if err != nil {
		utils.GetLogger(ctx).Errorf("create task manager %+v", err)
		return
	}
	esProxy.Run()

}
