package resourcecontroller

import (
	"context"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

type RequestUnit float64

type RequestInfo interface {
	IsWrite() bool
	WriteBytes() uint64
}

type ResponseInfo interface {
	ReadBytes() uint64
	KVCPUMs() uint64
}

func Sub(c float64, other float64) float64 {
	if c < other {
		return 0
	} else {
		return c - other
	}
}

type ResourceCalculator interface {
	Trickle(map[rmpb.ResourceType]float64, map[rmpb.RequestUnitType]float64, context.Context)
	BeforeKVRequest(map[rmpb.ResourceType]float64, map[rmpb.RequestUnitType]float64, RequestInfo)
	AfterKVRequest(map[rmpb.ResourceType]float64, map[rmpb.RequestUnitType]float64, RequestInfo, ResponseInfo)
}

type demoKVCalculator struct {
	*Config
}

func newDemoKVCalculator(cfg *Config) *demoKVCalculator {
	return &demoKVCalculator{Config: cfg}
}

func (dwc *demoKVCalculator) Trickle(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, ctx context.Context) {
}

func (dwc *demoKVCalculator) BeforeKVRequest(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, req RequestInfo) {
	if req.IsWrite() {
		resource[rmpb.ResourceType_KVWriteRPCCount] += 1

		writeBytes := req.WriteBytes()
		resource[rmpb.ResourceType_WriteBytes] += float64(writeBytes)

		ru[rmpb.RequestUnitType_WRU] += float64(dwc.WriteRequestCost)
		ru[rmpb.RequestUnitType_WRU] += float64(dwc.WriteBytesCost) * float64(writeBytes)
	} else {
		resource[rmpb.ResourceType_KVReadRPCCount] += 1
		ru[rmpb.RequestUnitType_RRU] += float64(dwc.ReadRequestCost)
	}
}
func (dwc *demoKVCalculator) AfterKVRequest(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, req RequestInfo, res ResponseInfo) {
	readBytes := res.ReadBytes()
	resource[rmpb.ResourceType_ReadBytes] += float64(readBytes)

	ru[rmpb.RequestUnitType_RRU] += float64(readBytes) * float64(dwc.ReadBytesCost)

	kvCPUms := float64(res.KVCPUMs())
	resource[rmpb.ResourceType_TotalCPUTimeMs] += kvCPUms
	if req.IsWrite() {
		ru[rmpb.RequestUnitType_WRU] += kvCPUms * float64(dwc.WriteCPUMsCost)
	} else {
		ru[rmpb.RequestUnitType_RRU] += kvCPUms * float64(dwc.ReadCPUMsCost)
	}
}

type demoSQLLayerCPUCalculateor struct {
	*Config
}

func newDemoSQLLayerCPUCalculateor(cfg *Config) *demoSQLLayerCPUCalculateor {
	return &demoSQLLayerCPUCalculateor{Config: cfg}
}

func (dsc *demoSQLLayerCPUCalculateor) Trickle(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, ctx context.Context) {
	cpu := UserCPUSecs(ctx)
	resource[rmpb.ResourceType_TotalCPUTimeMs] += cpu
	resource[rmpb.ResourceType_SQLLayerCPUTimeMs] += cpu
	// todo: ru custom
}

func (dsc *demoSQLLayerCPUCalculateor) BeforeKVRequest(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, req RequestInfo) {
}
func (dsc *demoSQLLayerCPUCalculateor) AfterKVRequest(resource map[rmpb.ResourceType]float64, ru map[rmpb.RequestUnitType]float64, req RequestInfo, res ResponseInfo) {
}
