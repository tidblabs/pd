package resourcecontroller

import (
	"context"
	"os"

	"github.com/elastic/gosigar"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// func getResourceTokenBucketFromSettings(settings *rmpb.GroupSettings, typ rmpb.ResourceType) *rmpb.TokenBucket {
// 	switch settings.Mode {
// 	case rmpb.GroupMode_RUMode:
// 		switch typ {
// 		case rmpb.ResourceType_RRU:
// 			return settings.RRU
// 		case rmpb.ResourceType_WRU:
// 			return settings.WRU
// 		}
// 	case rmpb.GroupMode_NativeMode:
// 		s
// 	}

// 	return nil
// }

// GetCPUTime returns the cumulative user/system time (in ms) since the process start.
func GetCPUTime(ctx context.Context) (userTimeMillis, sysTimeMillis int64, err error) {
	pid := os.Getpid()
	cpuTime := gosigar.ProcTime{}
	if err := cpuTime.Get(pid); err != nil {
		return 0, 0, err
	}
	return int64(cpuTime.User), int64(cpuTime.Sys), nil
}

func UserCPUSecs(ctx context.Context) (CPUSecs float64) {
	userTimeMillis, _, err := GetCPUTime(ctx)
	if err != nil {
		log.Error("Failed to get CPU time", zap.Error(err))
		return
	}
	CPUSecs = float64(userTimeMillis) * 1e-3
	return
}
