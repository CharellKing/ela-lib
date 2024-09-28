package es

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"strings"
)

type IESSettings interface {
	ToESV5Setting(targetIndex string) map[string]interface{}
	ToESV6Setting(targetIndex string) map[string]interface{}
	ToESV7Setting(targetIndex string) map[string]interface{}
	ToESV8Setting(targetIndex string) map[string]interface{}

	ToESV5Mapping(targetIndex string) map[string]interface{}
	ToESV6Mapping(targetIndex string) map[string]interface{}
	ToESV7Mapping(targetIndex string) map[string]interface{}
	ToESV8Mapping() map[string]interface{}

	ToTargetV5Settings(targetIndex string) *V5Settings
	ToTargetV6Settings(targetIndex string) *V6Settings
	ToTargetV7Settings(targetIndex string) *V7Settings
	ToTargetV8Settings(targetIndex string) *V8Settings

	GetIndex() string
	GetMappings() map[string]interface{}
	GetSettings() map[string]interface{}
	GetAliases() map[string]interface{}
	GetProperties() map[string]interface{}
	GetFieldMap() map[string]interface{}
}

func GetESSettings(esVersion string, settings map[string]interface{}) (IESSettings, error) {
	if strings.HasPrefix(esVersion, "8.") {
		var v8Settings V8Settings
		if err := mapstructure.Decode(settings, &v8Settings); err != nil {
			return nil, errors.WithStack(err)
		}
		return &v8Settings, nil
	} else if strings.HasPrefix(esVersion, "7.") {
		var v7Settings V7Settings
		if err := mapstructure.Decode(settings, &v7Settings); err != nil {
			return nil, errors.WithStack(err)
		}
		return &v7Settings, nil
	} else if strings.HasPrefix(esVersion, "6.") {
		var v6Settings V6Settings
		if err := mapstructure.Decode(settings, &v6Settings); err != nil {
			return nil, errors.WithStack(err)
		}
		return &v6Settings, nil
	} else if strings.HasPrefix(esVersion, "5.") {
		var v5Settings V5Settings
		if err := mapstructure.Decode(settings, &v5Settings); err != nil {
			return nil, errors.WithStack(err)
		}
		return &v5Settings, nil
	}
	return nil, fmt.Errorf("unsupported version: %s", esVersion)
}
