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
	KVCPUms() uint64
}

func Sub(c *rmpb.ResourceDetail, other *rmpb.ResourceDetail) {
	if c.Value < other.Value {
		c.Value = 0
	} else {
		c.Value -= other.Value
	}
}

func Add(c *rmpb.ResourceDetail, other *rmpb.ResourceDetail) {
	if other != nil {
		c.Value += other.Value
	}
}

type ResourceCalculator interface {
	Trickle(map[rmpb.ResourceType]*rmpb.ResourceDetail, context.Context)
	BeforeKVRequest(map[rmpb.ResourceType]*rmpb.ResourceDetail, RequestInfo)
	AfterKVRequest(map[rmpb.ResourceType]*rmpb.ResourceDetail, RequestInfo, ResponseInfo)
}

type demoKVCalculator struct {
	*Config
}

func newDemoKVCalculator(cfg *Config) *demoKVCalculator {
	return &demoKVCalculator{Config: cfg}
}

func (dwc *demoKVCalculator) Trickle(rs map[rmpb.ResourceType]*rmpb.ResourceDetail, ctx context.Context) {
}

func (dwc *demoKVCalculator) BeforeKVRequest(rs map[rmpb.ResourceType]*rmpb.ResourceDetail, req RequestInfo) {
	if req.IsWrite() {
		if _, ok := rs[rmpb.ResourceType_KVWriteRPCCount]; !ok {
			rs[rmpb.ResourceType_KVWriteRPCCount] = &rmpb.ResourceDetail{}
		}
		rs[rmpb.ResourceType_KVWriteRPCCount].Value++

		writeBytes := req.WriteBytes()
		if _, ok := rs[rmpb.ResourceType_WriteBytes]; !ok {
			rs[rmpb.ResourceType_WriteBytes] = &rmpb.ResourceDetail{}
		}
		rs[rmpb.ResourceType_WriteBytes].Value += writeBytes
		if _, ok := rs[rmpb.ResourceType_WRU]; !ok {
			rs[rmpb.ResourceType_WRU] = &rmpb.ResourceDetail{}
		}
		rs[rmpb.ResourceType_WRU].Value += uint64(dwc.WriteRequestCost)
		rs[rmpb.ResourceType_WRU].Value += writeBytes * uint64(dwc.WriteBytesCost)
	} else {
		if _, ok := rs[rmpb.ResourceType_KVReadRPCCount]; !ok {
			rs[rmpb.ResourceType_KVReadRPCCount] = &rmpb.ResourceDetail{}
		}
		rs[rmpb.ResourceType_KVReadRPCCount].Value++
		if _, ok := rs[rmpb.ResourceType_RRU]; !ok {
			rs[rmpb.ResourceType_RRU] = &rmpb.ResourceDetail{}
		}
		rs[rmpb.ResourceType_RRU].Value += uint64(dwc.ReadRequestCost)
	}
}
func (dwc *demoKVCalculator) AfterKVRequest(rs map[rmpb.ResourceType]*rmpb.ResourceDetail, req RequestInfo, res ResponseInfo) {
	readBytes := res.ReadBytes()
	if _, ok := rs[rmpb.ResourceType_ReadBytes]; !ok {
		rs[rmpb.ResourceType_ReadBytes] = &rmpb.ResourceDetail{}
	}
	rs[rmpb.ResourceType_ReadBytes].Value += readBytes

	if _, ok := rs[rmpb.ResourceType_RRU]; !ok {
		rs[rmpb.ResourceType_RRU] = &rmpb.ResourceDetail{}
	}
	rs[rmpb.ResourceType_RRU].Value += readBytes * uint64(dwc.ReadBytesCost)

	if _, ok := rs[rmpb.ResourceType_TotoalCPUTimeMs]; !ok {
		rs[rmpb.ResourceType_TotoalCPUTimeMs] = &rmpb.ResourceDetail{}
	}
	rs[rmpb.ResourceType_TotoalCPUTimeMs].Value += res.KVCPUms()
	if req.IsWrite() {
		if _, ok := rs[rmpb.ResourceType_WRU]; !ok {
			rs[rmpb.ResourceType_WRU] = &rmpb.ResourceDetail{}
		}
		rs[rmpb.ResourceType_WRU].Value += res.KVCPUms() * uint64(dwc.WriteCPUMsCost)
	} else {
		if _, ok := rs[rmpb.ResourceType_RRU]; !ok {
			rs[rmpb.ResourceType_RRU] = &rmpb.ResourceDetail{}
		}
		rs[rmpb.ResourceType_RRU].Value += res.KVCPUms() * uint64(dwc.ReadCPUMsCost)
	}
}

type demoSQLLayerCPUCalculateor struct {
	*Config
}

func newDemoSQLLayerCPUCalculateor(cfg *Config) *demoSQLLayerCPUCalculateor {
	return &demoSQLLayerCPUCalculateor{Config: cfg}
}

func (dsc *demoSQLLayerCPUCalculateor) Trickle(rs map[rmpb.ResourceType]*rmpb.ResourceDetail, ctx context.Context) {
	cpu := uint64(UserCPUSecs(ctx))
	if _, ok := rs[rmpb.ResourceType_TotoalCPUTimeMs]; !ok {
		rs[rmpb.ResourceType_TotoalCPUTimeMs] = &rmpb.ResourceDetail{}
	}
	rs[rmpb.ResourceType_TotoalCPUTimeMs].Value += cpu
	if _, ok := rs[rmpb.ResourceType_SQLLayerCPUTimeMs]; !ok {
		rs[rmpb.ResourceType_SQLLayerCPUTimeMs] = &rmpb.ResourceDetail{}
	}
	rs[rmpb.ResourceType_SQLLayerCPUTimeMs].Value += cpu
	// todo: ru custom
}

func (dsc *demoSQLLayerCPUCalculateor) BeforeKVRequest(rs map[rmpb.ResourceType]*rmpb.ResourceDetail, req RequestInfo) {
}
func (dsc *demoSQLLayerCPUCalculateor) AfterKVRequest(rs map[rmpb.ResourceType]*rmpb.ResourceDetail, req RequestInfo, res ResponseInfo) {
}
