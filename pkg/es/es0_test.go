package es

import (
	"context"
	"fmt"
	"github.com/CharellKing/ela-lib/config"
	"regexp"
	"testing"
)

func TestGetVersion(t *testing.T) {
	es0 := NewESV0(&config.ESConfig{
		Addresses: []string{
			"http://127.0.0.1:15200",
		},
		User:     "",
		Password: "",
	})

	clusterVersion, err := es0.GetVersion()
	if err != nil {
		t.Errorf("%+v", err)
		return
	}

	t.Logf("version: %+v", clusterVersion)
}

func TestGetClient(t *testing.T) {
	esConfig := &config.ESConfig{
		Addresses: []string{
			"http://127.0.0.1:8080",
		},
		User:     "",
		Password: "",
	}

	es0 := NewESV0(esConfig)

	client, err := es0.GetES()
	if err != nil {
		t.Errorf("%+v", err)
		return
	}

	ctx := context.Background()
	resp, err := client.ClusterHealth(ctx)
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(resp)
}

func TestPattern(t *testing.T) {
	// 定义正则表达式
	pattern := `/((\w)*)/((\w)*)/((\w)*)`
	re := regexp.MustCompile(pattern)

	// 要匹配的字符串
	str := "/abc/bcd/cef"

	// 检测字符串是否匹配
	if re.MatchString(str) {
		fmt.Println("String matches the pattern")

		// 提取匹配的子字符串
		matches := re.FindStringSubmatch(str)

		// 打印匹配结果
		for i, match := range matches {
			fmt.Printf("Match %d: %s\n", i, match)
		}
	} else {
		fmt.Println("String does not match the pattern")
	}

}
