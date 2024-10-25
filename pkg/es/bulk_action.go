package es

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"strings"
)

type DocTypeReservationType string

const (
	DocTypeReservationTypeCreate DocTypeReservationType = "add"
	DocTypeReservationTypeKeep   DocTypeReservationType = "keep"
	DocTypeReservationTypeDelete DocTypeReservationType = "delete"
)

type BulkRequestItem struct {
	ActionType string
	Metadata   map[string]interface{}
	Document   map[string]interface{}
}

func (bulkRequestItem *BulkRequestItem) ToStringArray() []string {
	metadataMap := map[string]interface{}{
		bulkRequestItem.ActionType: bulkRequestItem.Metadata,
	}

	var bulkActionArray []string
	metadataBytes, _ := json.Marshal(metadataMap)
	bulkActionArray = append(bulkActionArray, string(metadataBytes))

	var documentBytes []byte
	if bulkRequestItem.ActionType == "update" {
		documentBytes, _ = json.Marshal(map[string]interface{}{
			"doc": bulkRequestItem.Document,
		})
	} else if bulkRequestItem.Document != nil {
		documentBytes, _ = json.Marshal(bulkRequestItem.Document)
	}

	if documentBytes != nil {
		bulkActionArray = append(bulkActionArray, string(documentBytes))
	}

	return bulkActionArray
}

func parseRequest(bodyBytes []byte, reservationType DocTypeReservationType) ([]*BulkRequestItem, error) {
	scanner := bufio.NewScanner(bytes.NewReader(bodyBytes))
	var actionList []*BulkRequestItem

	var currentAction *BulkRequestItem
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var jsonMap map[string]interface{}
		err := json.Unmarshal([]byte(line), &jsonMap)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if currentAction == nil {
			currentAction = &BulkRequestItem{}
			currentAction.ActionType, currentAction.Metadata = utils.GetFirstKeyMapValue(jsonMap)
			if reservationType == DocTypeReservationTypeDelete {
				delete(currentAction.Metadata, "_type")
			} else if reservationType == DocTypeReservationTypeCreate {
				currentAction.Metadata["_type"] = "_doc"
			}
			if currentAction.ActionType == "delete" {
				actionList = append(actionList, currentAction)
				currentAction = nil
			}
		} else {
			if currentAction.ActionType == "update" {
				_, currentAction.Document = utils.GetFirstKeyMapValue(jsonMap)
			} else {
				currentAction.Document = jsonMap
			}
			actionList = append(actionList, currentAction)
			currentAction = nil
		}
	}
	return actionList, nil
}

type BulkResponseItem struct {
	Id     string `mapstructure:"_id"`
	Status int    `mapstructure:"status"`
}

type BulkResponse struct {
	Items []map[string]BulkResponseItem `mapstructure:"items"`
}

func parseResponse(response map[string]interface{}) ([]*BulkResponseItem, error) {
	var bulkResponse BulkResponse
	_ = mapstructure.Decode(&response, &bulkResponse)

	var bulkResponseItems []*BulkResponseItem
	for _, itemMap := range bulkResponse.Items {
		for _, bulkResponseItem := range itemMap {
			bulkResponseItems = append(bulkResponseItems, &bulkResponseItem)
		}
	}
	return bulkResponseItems, nil
}

func adjustBulkRequest(bulkRequestList []*BulkRequestItem, bulkResponseList []*BulkResponseItem) ([]*BulkRequestItem, error) {
	if len(bulkRequestList) != len(bulkResponseList) {
		return nil, fmt.Errorf("request items amount is not equal to response items")
	}

	var newBulkRequestList []*BulkRequestItem
	for i, bulkResponse := range bulkResponseList {
		if bulkResponse.Status > 299 {
			continue

		}

		if lo.Contains([]string{"create", "index", "update"}, bulkRequestList[i].ActionType) {
			bulkRequestList[i].Metadata["_id"] = bulkResponse.Id
			//bulkRequestList[i].Document["_id"] = bulkResponse.Id
			bulkRequestList[i].ActionType = "index"
		}

		newBulkRequestList = append(newBulkRequestList, bulkRequestList[i])
	}
	return newBulkRequestList, nil
}

func AdjustBulkRequestBodyWithOnlyDocType(requestBody []byte, reservationType DocTypeReservationType) ([]byte, error) {
	if reservationType == DocTypeReservationTypeKeep {
		return requestBody, nil
	}

	bulkRequestItems, err := parseRequest(requestBody, reservationType)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var newBulkRequestItemStringArray []string
	for _, bulkRequestItem := range bulkRequestItems {
		newBulkRequestItemStringArray = append(newBulkRequestItemStringArray, bulkRequestItem.ToStringArray()...)
	}

	newBulkRequestItemStringArray = append(newBulkRequestItemStringArray, "")
	newBulkRequestBodyString := strings.Join(newBulkRequestItemStringArray, "\n")
	return []byte(newBulkRequestBodyString), nil
}

func AdjustBulkRequestBody(requestBody []byte, responseBody map[string]interface{}, reservationType DocTypeReservationType) ([]byte, error) {
	bulkRequestItems, err := parseRequest(requestBody, reservationType)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	bulkResponseItems, err := parseResponse(responseBody)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	newBulkRequestItems, err := adjustBulkRequest(bulkRequestItems, bulkResponseItems)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var newBulkRequestItemStringArray []string
	for _, bulkRequestItem := range newBulkRequestItems {
		newBulkRequestItemStringArray = append(newBulkRequestItemStringArray, bulkRequestItem.ToStringArray()...)
	}

	newBulkRequestItemStringArray = append(newBulkRequestItemStringArray, "")
	newBulkRequestBodyString := strings.Join(newBulkRequestItemStringArray, "\n")
	return []byte(newBulkRequestBodyString), nil
}
