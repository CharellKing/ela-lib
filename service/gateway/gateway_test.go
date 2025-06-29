package gateway

import (
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"github.com/CharellKing/ela-lib/utils"
	log "github.com/sirupsen/logrus"
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
	esProxy, err := NewESGateway(&cfg)
	if err != nil {
		log.Errorf("create task manager %+v", err)
		return
	}
	esProxy.Run()

}
