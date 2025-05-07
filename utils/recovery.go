package utils

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"runtime"
)

func GoRecovery(ctx context.Context, f func()) {
	defer Recovery(ctx)
	go f()
}

func Recovery(ctx context.Context) {
	if err := recover(); err != nil {
		buf := make([]byte, 64<<10) //nolint:gomnd
		n := runtime.Stack(buf, false)
		buf = buf[:n]
		log.Errorf("panic recovered, err: %+v\n stack: %+v", err, cast.ToString(buf))
	}
}
