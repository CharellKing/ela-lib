package es

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/CharellKing/ela-lib/utils"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	_ "github.com/pkg/errors"
	"github.com/spf13/cast"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strings"
)

type BaseES struct {
	ClusterVersion string
	Addresses      []string
	User           string
	Password       string

	ActionRuleMap map[RequestActionType]*UriParserRule
	MethodRuleMap map[MethodType][]*MatchRule

	Settings IESSettings
}

func NewBaseES(clusterVersion string, addresses []string, user string, password string) *BaseES {
	baseES := &BaseES{
		ClusterVersion: clusterVersion,
		Addresses:      addresses,
		User:           user,
		Password:       password,
	}

	baseES.GetActionRuleMap()
	baseES.GetMethodRuleMap()
	return baseES
}

func (es *BaseES) ClusterVersionGte7() bool {
	segments := strings.Split(es.ClusterVersion, ".")
	return cast.ToInt(segments[0]) >= 7
}

func (es *BaseES) GetActionRuleMap() map[RequestActionType]*UriParserRule {
	if len(es.ActionRuleMap) > 0 {
		return es.ActionRuleMap
	}

	var originActionRuleMap map[RequestActionType]*UriParserRule
	if strings.HasPrefix(es.ClusterVersion, "5.") {
		originActionRuleMap = getUriParserRuleMapWithRequestActionForV5()
	} else if strings.HasPrefix(es.ClusterVersion, "6.") {
		originActionRuleMap = getUriParserRuleMapWithRequestActionForV6()
	} else if strings.HasPrefix(es.ClusterVersion, "7.") {
		originActionRuleMap = getUriParserRuleMapWithRequestActionForV7()
	} else if strings.HasPrefix(es.ClusterVersion, "8.") {
		originActionRuleMap = getUriParserRuleMapWithRequestActionForV8()
	} else {
		return nil
	}

	_ = copier.Copy(&es.ActionRuleMap, &originActionRuleMap)
	for requestActionType, rule := range es.ActionRuleMap {
		sort.Slice(rule.MatchRules, func(i, j int) bool {
			return rule.MatchRules[i].Priority < rule.MatchRules[j].Priority
		})

		for _, matchRule := range rule.MatchRules {
			matchRule.RequestActionType = requestActionType
		}
	}
	return es.ActionRuleMap
}

func (es *BaseES) GetMethodRuleMap() map[MethodType][]*MatchRule {
	if len(es.MethodRuleMap) > 0 {
		return es.MethodRuleMap
	}

	es.MethodRuleMap = make(map[MethodType][]*MatchRule)

	for _, actionRule := range es.ActionRuleMap {
		for _, matchRule := range actionRule.MatchRules {
			if _, ok := es.MethodRuleMap[matchRule.Method]; !ok {
				es.MethodRuleMap[matchRule.Method] = make([]*MatchRule, 0)
			}
			es.MethodRuleMap[matchRule.Method] = append(es.MethodRuleMap[matchRule.Method], matchRule)
		}

	}

	for _, matchRules := range es.MethodRuleMap {
		sort.Slice(matchRules, func(i, j int) bool {
			return matchRules[i].Priority < matchRules[j].Priority
		})
	}

	return es.MethodRuleMap
}

func (es *BaseES) GetAddresses() []string {
	return es.Addresses
}

func (es *BaseES) GetUser() string {
	return es.User
}

func (es *BaseES) GetPassword() string {
	return es.Password
}

func (es *BaseES) matchRule(uri string, uriPattern string) (map[string]string, bool) {
	uri = strings.Trim(uri, "/")
	uriPattern = strings.Trim(uriPattern, "/")

	patternSegments := strings.Split(uriPattern, "/")
	patternSegmentLen := len(patternSegments)
	suffixPatternAction := ""
	removeActionPatternSegments := patternSegments
	if strings.HasPrefix(patternSegments[patternSegmentLen-1], "_") {
		suffixPatternAction = patternSegments[patternSegmentLen-1]
		removeActionPatternSegments = patternSegments[:patternSegmentLen-1]
	}

	uriSegments := strings.Split(uri, "/")
	uriSegmentLen := len(uriSegments)
	suffixUriAction := ""
	removeActionUriSegments := uriSegments
	if suffixPatternAction != "" && strings.HasPrefix(uriSegments[uriSegmentLen-1], "_") {
		suffixUriAction = uriSegments[uriSegmentLen-1]
		removeActionUriSegments = uriSegments[:uriSegmentLen-1]
	}

	if suffixPatternAction != suffixUriAction {
		return nil, false
	}

	var (
		uriBeginIdx = 0
		uriEndIdx   = len(removeActionUriSegments) - 1

		patternBeginIdx = 0
		patternEndIdx   = len(removeActionPatternSegments) - 1
	)

	variableMap := make(map[string]string)
	hasChange := true
	for uriBeginIdx <= uriEndIdx && patternBeginIdx <= patternEndIdx && hasChange {
		hasChange = false
		if !strings.HasSuffix(removeActionPatternSegments[patternBeginIdx], "?") {
			variable := strings.Trim(removeActionPatternSegments[patternBeginIdx], "${}")
			variableMap[variable] = removeActionUriSegments[uriBeginIdx]
			uriBeginIdx++
			patternBeginIdx++
			hasChange = true
		}

		if uriBeginIdx <= uriEndIdx && patternBeginIdx <= patternEndIdx {
			if !strings.HasSuffix(removeActionPatternSegments[patternEndIdx], "?") {
				variable := strings.Trim(removeActionPatternSegments[patternEndIdx], "${}")
				variableMap[variable] = removeActionUriSegments[uriEndIdx]
				uriEndIdx--
				patternEndIdx--
				hasChange = true
			}
		}
	}

	for patternBeginIdx <= patternEndIdx {
		if !strings.HasSuffix(removeActionPatternSegments[patternBeginIdx], "?") {
			return nil, false
		}
		variable := strings.Trim(removeActionPatternSegments[patternBeginIdx], "${}?")

		if uriBeginIdx <= uriEndIdx {
			variableMap[variable] = removeActionUriSegments[uriBeginIdx]
			uriBeginIdx++
		}
		patternBeginIdx++
	}

	if uriBeginIdx <= uriEndIdx {
		return nil, false
	}

	return variableMap, true
}

func (es *BaseES) MatchRule(c *gin.Context) *UriPathParserResult {
	for _, matchRule := range es.MethodRuleMap[MethodType(c.Request.Method)] {
		if string(matchRule.Method) != c.Request.Method {
			continue
		}
		variableMap, ok := es.matchRule(c.Request.URL.Path, matchRule.UriPattern)
		if ok {
			if es.ClusterVersionGte7() {
				if _, ok := variableMap["index"]; ok {
					variableMap["docType"] = "_doc"
				}
			}
			return &UriPathParserResult{
				VariableMap:   variableMap,
				RequestAction: matchRule.RequestActionType,
			}
		}
	}

	variableMap := make(map[string]string)
	if es.ClusterVersionGte7() {
		if _, ok := variableMap["index"]; ok {
			variableMap["docType"] = "_doc"
		}
	}

	return &UriPathParserResult{
		VariableMap:   variableMap,
		RequestAction: RequestActionType(c.Request.Method),
	}
}

func (es *BaseES) MakeUri(c *gin.Context, uriPathParserResult *UriPathParserResult) (*UriPathMakeResult, error) {
	actionRule, ok := es.ActionRuleMap[uriPathParserResult.RequestAction]
	if !ok {
		utils.GetLogger(c).Warnf("action rule not found %s", uriPathParserResult.RequestAction)

		return &UriPathMakeResult{
			Uri:    c.Request.RequestURI,
			Method: MethodType(c.Request.Method),

			Address:  es.Addresses[rand.Intn(len(es.Addresses))],
			User:     es.User,
			Password: es.Password,
		}, nil
	}

	var (
		uri           string
		bestMatchRule *MatchRule
	)

	for _, matchRule := range actionRule.MatchRules {
		var uriSegments []string
		patternSegments := strings.Split(matchRule.UriPattern, "/")

		isOk := true
		for _, patternSegment := range patternSegments {
			if strings.HasPrefix(patternSegment, "${") {
				variable := strings.Trim(patternSegment, "${}?")
				value, ok := uriPathParserResult.VariableMap[variable]
				if !ok {
					if !strings.HasSuffix(patternSegment, "?") {
						isOk = false
						break
					}
				} else {
					uriSegments = append(uriSegments, value)
				}
			} else {
				uriSegments = append(uriSegments, patternSegment)
			}
		}

		if isOk {
			uri = strings.Join(uriSegments, "/")
			bestMatchRule = matchRule
			break
		}
	}

	if bestMatchRule == nil {
		return nil, fmt.Errorf("uri make %s", uriPathParserResult.RequestAction)
	}

	return &UriPathMakeResult{
		Uri:    uri,
		Method: bestMatchRule.Method,

		Address:  es.Addresses[rand.Intn(len(es.Addresses))],
		User:     es.User,
		Password: es.Password,
	}, nil
}

func (es *BaseES) GetSearchResponse(bodyMap map[string]interface{}) map[string]interface{} {
	if es.ClusterVersionGte7() {
		totalValue, ok := utils.GetValueFromMapByPath(bodyMap, "hits.total.value")
		if !ok {
			totalValue, _ = utils.GetValueFromMapByPath(bodyMap, "hits.total")
			utils.SetValueFromMapByPath(bodyMap, "hit.total", map[string]interface{}{
				"value":    totalValue,
				"relation": "eq",
			})
		}
	} else {
		totalValue, ok := utils.GetValueFromMapByPath(bodyMap, "hits.total.value")
		if ok {
			utils.SetValueFromMapByPath(bodyMap, "hit.total", totalValue)
		}
	}
	return bodyMap
}

func (es *BaseES) IsWrite(requestActionType RequestActionType) bool {
	actionRule, ok := es.ActionRuleMap[requestActionType]
	if !ok {
		return false
	}
	return actionRule.IsWrite
}

func (es *BaseES) Request(c *gin.Context, bodyBytes []byte, parserUriResult *UriPathParserResult) (map[string]interface{}, int, error) {
	makeUriResult, err := es.MakeUri(c, parserUriResult)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.WithStack(err)
	}

	targetUrl := fmt.Sprintf("%s%s", makeUriResult.Address, makeUriResult.Uri)

	req, err := http.NewRequest(string(makeUriResult.Method), targetUrl, io.NopCloser(bytes.NewBuffer(bodyBytes)))
	if err != nil {
		return nil, http.StatusInternalServerError, errors.WithStack(err)
	}

	req.SetBasicAuth(makeUriResult.User, makeUriResult.Password)
	for k, v := range c.Request.Header {
		if k == "Accept-Encoding" {
			continue
		}

		req.Header.Set(k, v[0])
	}

	req.Header.Set("Content-Type", "application/json")

	reqQuery := req.URL.Query()

	queryParams := c.Request.URL.Query()
	for key, values := range queryParams {
		for _, value := range values {
			reqQuery.Add(key, value)
		}
	}

	client := &http.Client{}
	resp, err := client.Do(req)

	if resp != nil {
		for k, v := range resp.Header {
			c.Header(k, v[0])
		}
	}

	if err != nil {
		return nil, http.StatusInternalServerError, errors.WithStack(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode > 299 {
		return es.formatResponse(resp)
	}

	bodyMap, statusCode, _ := es.formatResponse(resp)

	return bodyMap, statusCode, nil
}

func (es *BaseES) formatResponse(resp *http.Response) (map[string]interface{}, int, error) {
	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyMap := make(map[string]interface{})
	_ = json.Unmarshal(bodyBytes, &bodyMap)
	return bodyMap, resp.StatusCode, nil
}
