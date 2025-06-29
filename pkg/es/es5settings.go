package es

import (
	"fmt"
	"github.com/jinzhu/copier"
	"github.com/samber/lo"
	path "github.com/segment-boneyard/go-map-path"
	"github.com/spf13/cast"
	"sort"
)

type V5Settings struct {
	Settings map[string]interface{}
	Mappings map[string]interface{}
	Aliases  map[string]interface{}

	SourceIndex string
}

func NewV5Settings(settings, mappings, aliases map[string]interface{}, sourceIndex string) *V5Settings {
	return &V5Settings{
		Settings:    settings,
		Mappings:    mappings,
		Aliases:     aliases,
		SourceIndex: sourceIndex,
	}
}

func (v5 *V5Settings) getUnwrappedSettings() map[string]interface{} {
	var copySourceSettings map[string]interface{}
	_ = copier.Copy(&copySourceSettings, v5.Settings)

	unwrappedSettingMap := cast.ToStringMap(path.Path(copySourceSettings, fmt.Sprintf("%s.settings.index", v5.SourceIndex)))
	unsupportedKey := []string{"provided_name", "creation_date", "uuid", "version"}
	for _, key := range unsupportedKey {
		delete(unwrappedSettingMap, key)
	}
	return unwrappedSettingMap
}

func (v5 *V5Settings) ToESV5Setting() map[string]interface{} {
	return map[string]interface{}{
		"settings": map[string]interface{}{
			"index": v5.getUnwrappedSettings(),
		},
	}
}

func (v5 *V5Settings) ToESV6Setting() map[string]interface{} {
	return v5.ToESV5Setting()
}

func (v5 *V5Settings) ToESV7Setting() map[string]interface{} {
	return v5.ToESV5Setting()
}

func (v5 *V5Settings) ToESV8Setting() map[string]interface{} {
	unwrappedSetting := v5.getUnwrappedSettings()
	return map[string]interface{}{
		"settings": unwrappedSetting,
	}
}

func (v5 *V5Settings) getUnwrappedMappings() map[string]interface{} {
	var copySourceMappings map[string]interface{}
	_ = copier.Copy(&copySourceMappings, v5.Mappings)

	unwrappedMappings := cast.ToStringMap(path.Path(v5.Mappings, fmt.Sprintf("%s.mappings", v5.SourceIndex)))
	return unwrappedMappings
}

func (v5 *V5Settings) mergeUnWrappedMapping(unwrappedMappings map[string]interface{}) map[string]interface{} {
	var typePropertiesMapArray []map[string]interface{}
	for _, typeProperties := range unwrappedMappings {
		typePropertiesMap := cast.ToStringMap(typeProperties)
		if _, ok := typePropertiesMap["properties"]; !ok {
			continue
		}

		enabled := path.Path(typePropertiesMap, "_source.enabled")
		if enabled != nil && cast.ToBool(enabled) == false {
			continue
		}

		typePropertiesMapArray = append(typePropertiesMapArray, cast.ToStringMap(typePropertiesMap["properties"]))
	}

	sort.Slice(typePropertiesMapArray, func(i, j int) bool {
		return len(typePropertiesMapArray[i]) > len(typePropertiesMapArray[j])
	})

	mergedProperties := make(map[string]interface{})
	for _, typePropertiesMap := range typePropertiesMapArray {
		for key, value := range typePropertiesMap {
			mergedProperties[key] = value
		}
	}

	return map[string]interface{}{
		"properties": mergedProperties,
	}
}

func (v5 *V5Settings) ToESV5Mapping() map[string]interface{} {
	unwrappedMappings := v5.getUnwrappedMappings()
	return map[string]interface{}{
		"mappings": unwrappedMappings,
	}
}

func (v5 *V5Settings) ToAlias() map[string]interface{} {
	return cast.ToStringMap(v5.Aliases[v5.SourceIndex])
}

func (v5 *V5Settings) ToESV6Mapping() map[string]interface{} {
	return v5.ToESV5Mapping()
}

func (v5 *V5Settings) ToESV7Mapping() map[string]interface{} {
	return v5.ToESV8Mapping()
}

func (v5 *V5Settings) DateFieldSupportTimestamp(properties map[string]interface{}) map[string]interface{} {
	fieldMap := cast.ToStringMap(properties["properties"])
	for field, fieldAttr := range fieldMap {
		fieldAttrMap := cast.ToStringMap(fieldAttr)
		fieldType := cast.ToString(fieldAttrMap["type"])
		if fieldType != "date" {
			continue
		}

		fieldFormat := cast.ToString(fieldAttrMap["format"])
		if fieldFormat != "yyyy-MM-dd HH:mm:ss" {
			continue
		}

		fieldAttrMap["format"] = "yyyy-MM-dd HH:mm:ss||epoch_millis"

		fieldMap[field] = fieldAttrMap
	}

	return map[string]interface{}{
		"properties": fieldMap,
	}
}

func (v5 *V5Settings) ToESV8Mapping() map[string]interface{} {
	unwrappedMappings := v5.getUnwrappedMappings()
	mergedProperties := v5.mergeUnWrappedMapping(unwrappedMappings)
	mergedProperties = v5.DateFieldSupportTimestamp(mergedProperties)
	return map[string]interface{}{
		"mappings": mergedProperties,
	}
}

func (v5 *V5Settings) ToTargetV5Settings(targetIndex string) *V5Settings {
	return NewV5Settings(
		v5.ToESV5Setting(),
		v5.ToESV5Mapping(),
		v5.ToAlias(),
		targetIndex)
}

func (v5 *V5Settings) ToTargetV6Settings(targetIndex string) *V6Settings {
	return NewV6Settings(
		v5.ToESV6Setting(),
		v5.ToESV6Mapping(),
		v5.ToAlias(),
		targetIndex)
}

func (v5 *V5Settings) ToTargetV7Settings(targetIndex string) *V7Settings {
	return NewV7Settings(
		v5.ToESV7Setting(),
		v5.ToESV7Mapping(),
		v5.ToAlias(),
		targetIndex)
}

func (v5 *V5Settings) ToTargetV8Settings(targetIndex string) *V8Settings {
	return NewV8Settings(
		v5.ToESV8Setting(),
		v5.ToESV8Mapping(),
		v5.ToAlias(),

		targetIndex)
}

func (v5 *V5Settings) GetIndex() string {
	return v5.SourceIndex
}

func (v5 *V5Settings) GetMappings() map[string]interface{} {
	return v5.Mappings
}

func (v5 *V5Settings) GetSettings() map[string]interface{} {
	return v5.Settings
}

func (v5 *V5Settings) GetAliases() map[string]interface{} {
	aliasesMap := cast.ToStringMap(v5.Aliases["aliases"])
	return cast.ToStringMap(aliasesMap[v5.SourceIndex])
}

func (v5 *V5Settings) GetProperties() map[string]interface{} {
	return v5.mergeUnWrappedMapping(v5.getUnwrappedMappings())
}

func (v5 *V5Settings) GetFieldMap() map[string]interface{} {
	if v5 == nil {
		return nil
	}
	properties := v5.GetProperties()
	return cast.ToStringMap(properties["properties"])
}

func (v5 *V5Settings) ToV5TemplateSettings(patterns []string, order int) map[string]interface{} {
	return lo.Assign(v5.ToESV5Setting(), v5.ToESV5Mapping(), v5.ToAlias(), map[string]interface{}{
		"order":          order,
		"index_patterns": patterns,
	})
}

func (v5 *V5Settings) ToV6TemplateSettings(patterns []string, order int) map[string]interface{} {
	return lo.Assign(v5.ToESV6Setting(), v5.ToESV6Mapping(), v5.ToAlias(), map[string]interface{}{
		"order":          order,
		"index_patterns": patterns,
	})
}

func (v5 *V5Settings) ToV7TemplateSettings(patterns []string, order int) map[string]interface{} {
	return lo.Assign(v5.ToESV7Setting(), v5.ToESV7Mapping(), v5.ToAlias(), map[string]interface{}{
		"order":          order,
		"index_patterns": patterns,
	})
}

func (v5 *V5Settings) ToV8TemplateSettings(patterns []string, order int) map[string]interface{} {
	return lo.Assign(v5.ToESV8Setting(), v5.ToESV8Mapping(), v5.ToAlias(), map[string]interface{}{
		"order":          order,
		"index_patterns": patterns,
	})
}
