package utils

import (
	"math"
	"sync/atomic"
)

func ZeroAtomicUint64() *atomic.Uint64 {
	return &atomic.Uint64{}
}

func MaxAtomicUint64() *atomic.Uint64 {
	var maxCount atomic.Uint64
	maxCount.Store(math.MaxUint64)
	return &maxCount
}
