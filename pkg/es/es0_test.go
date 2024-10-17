package es

import (
	"context"
	"github.com/CharellKing/ela-lib/config"
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
