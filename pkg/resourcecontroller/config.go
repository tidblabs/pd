package resourcecontroller

import (
	"time"

	"github.com/pingcap/kvproto/pkg/resource_manager"
)

var typeLen = len(resource_manager.ResourceType_name)
var allowResourceList map[resource_manager.ResourceType]struct{} = map[resource_manager.ResourceType]struct{}{
	resource_manager.ResourceType_RRU: {},
	resource_manager.ResourceType_WRU: {},
}

const initialRquestUnits = 10000

const bufferRUs = 5000

// movingAvgFactor is the weight applied to a new "sample" of RU usage (with one
// sample per mainLoopUpdateInterval).
//
// If we want a factor of 0.5 per second, this should be:
//
//	0.5^(1 second / mainLoopUpdateInterval)
const movingAvgFactor = 0.5

const notifyFraction = 0.1

const consumptionsReportingThreshold = 100

const extendedReportingPeriodFactor = 4

const defaultGroupLoopUpdateInterval = 1 * time.Second
const defaultTargetPeriod = 10 * time.Second
const (
	readRequestCost  = 1
	readCostPerMB    = 0.5
	writeRequestCost = 5
	writeCostPerMB   = 200
	readCPUMsCost    = 1
	writeCPUMsCost   = 1
	sqlCPUSecondCost = 1
)

type Config struct {
	groupLoopUpdateInterval time.Duration
	targetPeriod            time.Duration

	ReadRequestCost  RequestUnit
	ReadBytesCost    RequestUnit
	ReadCPUMsCost    RequestUnit
	WriteRequestCost RequestUnit
	WriteBytesCost   RequestUnit
	WriteCPUMsCost   RequestUnit
	SQLCPUSecondCost RequestUnit
}

func DefaultConfig() *Config {
	cfg := &Config{
		groupLoopUpdateInterval: defaultGroupLoopUpdateInterval,
		targetPeriod:            defaultTargetPeriod,
		ReadRequestCost:         RequestUnit(readRequestCost),
		ReadBytesCost:           RequestUnit(readCostPerMB),
		ReadCPUMsCost:           RequestUnit(readCPUMsCost),
		WriteRequestCost:        RequestUnit(writeRequestCost),
		WriteBytesCost:          RequestUnit(writeCostPerMB),
		WriteCPUMsCost:          RequestUnit(writeCPUMsCost),
		SQLCPUSecondCost:        RequestUnit(sqlCPUSecondCost),
	}
	return cfg
}
