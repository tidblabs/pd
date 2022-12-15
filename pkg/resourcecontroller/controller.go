package resourcecontroller

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ResourceGroupProvider interface {
	ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error)
	GetResourceGroup(ctx context.Context, resourceGroupName string) (*rmpb.ResourceGroup, error)
	AddResourceGroup(ctx context.Context, resourceGroupName string, settings *rmpb.GroupSettings) ([]byte, error)
	AcquireTokenBuckets(ctx context.Context, resourceGroupName string, targetRequestPeriodMs uint64, requestedResource []*rmpb.ResourceDetail, consumptionSinceLastRequest []rmpb.ResourceDetail) ([]*rmpb.GrantedTokenBucket, error)
}

func NewResourceGroupController(
	provider ResourceGroupProvider,
) (*resourceGroupsController, error) {
	return newTenantSideCostController(provider)
}

type resourceGroupsController struct {
	instanceFingerprint string
	provider            ResourceGroupProvider
	groupsController    sync.Map
	config              *Config
}

func newTenantSideCostController(provider ResourceGroupProvider) (*resourceGroupsController, error) {
	return &resourceGroupsController{provider: provider, config: DefaultConfig()}, nil
}

func (c *resourceGroupsController) Start(ctx context.Context, instanceFingerprint string) error {
	if len(instanceFingerprint) == 0 {
		return errors.New("invalid SQLInstanceID")
	}
	c.instanceFingerprint = instanceFingerprint
	if err := c.addDemoResourceGroup(ctx); err != nil {
		log.Error("add Demo ResourceGroup failed", zap.Error(err))
	}
	if err := c.updateAllResourceGroups(ctx); err != nil {
		log.Error("update ResourceGroup failed", zap.Error(err))
	}
	return nil
}

func (c *resourceGroupsController) addDemoResourceGroup(ctx context.Context) error {
	setting := &rmpb.GroupSettings{
		RRU: &rmpb.TokenBucket{
			Tokens: 10000,
			Settings: &rmpb.TokenLimitSettings{
				Fillrate:   10,
				BurstLimit: 1000000,
			},
		},
		WRU: &rmpb.TokenBucket{
			Tokens: 1000,
			Settings: &rmpb.TokenLimitSettings{
				Fillrate:   10,
				BurstLimit: 1000000,
			},
		},
		ReadBandwidth: &rmpb.TokenBucket{
			Tokens: 1000000,
			Settings: &rmpb.TokenLimitSettings{
				Fillrate:   10,
				BurstLimit: 1000000,
			},
		},
		WriteBandwidth: &rmpb.TokenBucket{
			Tokens: 100000,
			Settings: &rmpb.TokenLimitSettings{
				Fillrate:   10,
				BurstLimit: 1000000,
			},
		},
	}
	context, err := c.provider.AddResourceGroup(ctx, "demo", setting)
	if err != nil {
		return err
	}
	log.Info("add resource group", zap.String("resp", string(context)))
	return err
}

func (c *resourceGroupsController) updateAllResourceGroups(ctx context.Context) error {
	groups, err := c.provider.ListResourceGroups(ctx)
	if err != nil {
		return err
	}
	lastedGroups := make(map[string]struct{})
	for _, group := range groups {
		log.Info("create resource group cost controller", zap.String("name", group.ResourceGroupName))
		gc := newGroupCostController(ctx, group, c.config, c.provider)
		c.groupsController.Store(group.GetResourceGroupName(), gc)
		lastedGroups[group.GetResourceGroupName()] = struct{}{}
	}
	c.groupsController.Range(func(key, value any) bool {
		resourceGroupName := key.(string)
		if _, ok := lastedGroups[resourceGroupName]; !ok {
			value.(*groupCostController).ctxCancel()
			c.groupsController.Delete(key)
		}
		return true
	})
	return nil
}

func (c *resourceGroupsController) OnRequestWait(
	ctx context.Context, resourceGroupName string, info RequestInfo,
) error {
	tmp, ok := c.groupsController.Load(resourceGroupName)
	if !ok {
		return errors.Errorf("[resource group] resourceGroupName %s is not existed.", resourceGroupName)
	}
	gc := tmp.(*groupCostController)
	err := gc.OnRequestWait(ctx, info)
	return err
}

func (c *resourceGroupsController) OnResponse(ctx context.Context, resourceGroupName string, req RequestInfo, resp ResponseInfo) {
	tmp, ok := c.groupsController.Load(resourceGroupName)
	if !ok {
		log.Warn("[resource group] resourceGroupName is not existed.", zap.String("resourceGroupName", resourceGroupName))
	}
	gc := tmp.(*groupCostController)
	gc.OnResponse(ctx, req, resp)
}

type groupCostController struct {
	ctxCancel         context.CancelFunc
	resourceGroupName string
	provider          ResourceGroupProvider
	mainCfg           *Config
	groupSettings     *rmpb.GroupSettings
	calculators       []ResourceCalculator

	mu struct {
		sync.Mutex
		consumptions []rmpb.ResourceDetail
	}

	// responseChan is used to receive results from token bucket requests, which
	// are run in a separate goroutine. A nil response indicates an error.
	responseChan chan []*rmpb.GrantedTokenBucket

	// lowRUNotifyChan is used when the number of available resource is running low and
	// we need to send an early token bucket request.
	lowRUNotifyChan chan struct{}

	// run contains the state that is updated by the main loop.
	run struct {
		now time.Time

		// consumptions stores the last value of mu.consumption.
		consumptions []rmpb.ResourceDetail

		lastRequestTime         time.Time
		lastReportedConsumption []rmpb.ResourceDetail

		// targetPeriod stores the value of the TargetPeriodSetting setting at the
		// last update.
		targetPeriod time.Duration

		// initialRequestCompleted is set to true when the first token bucket
		// request completes successfully.
		initialRequestCompleted bool

		// requestInProgress is true if we are in the process of sending a request;
		// it gets set to false when we process the response (in the main loop),
		// even in error cases.
		requestInProgress bool

		// requestNeedsRetry is set if the last token bucket request encountered an
		// error. This triggers a retry attempt on the next tick.
		//
		// Note: requestNeedsRetry and requestInProgress are never true at the same
		// time.
		requestNeedsRetry bool

		resourceTokens map[rmpb.ResourceType]*resourceTokenCounter
	}
}

type resourceTokenCounter struct {
	// avgRUPerSec is an exponentially-weighted moving average of the RU
	// consumption per second; used to estimate the RU requirements for the next
	// request.
	avgRUPerSec float64
	// lastSecRU is the consumption.RU value when avgRUPerSec was last updated.
	avgRUPerSecLastRU uint64
	avgLastTime       time.Time

	setupNotificationCh        <-chan time.Time
	setupNotificationThreshold float64
	setupNotificationTimer     *time.Timer
	limiter                    *Limiter

	lastDeadline time.Time
	lastRate     float64
	// lastBrust    int
}

func newGroupCostController(ctx context.Context, group *rmpb.ResourceGroup, mainCfg *Config, provider ResourceGroupProvider) *groupCostController {
	controllerCtx, controllerCancel := context.WithCancel(ctx)
	gc := &groupCostController{
		resourceGroupName: group.GetResourceGroupName(),
		ctxCancel:         controllerCancel,
		provider:          provider,
		mainCfg:           mainCfg,
		groupSettings:     group.Settings,
		responseChan:      make(chan []*rmpb.GrantedTokenBucket, 1),
		lowRUNotifyChan:   make(chan struct{}, 1),
		calculators:       []ResourceCalculator{newDemoKVCalculator(mainCfg), newDemoSQLLayerCPUCalculateor(mainCfg)},
	}

	gc.mu.consumptions = make([]rmpb.ResourceDetail, typeLen)

	gc.run.consumptions = make([]rmpb.ResourceDetail, typeLen)
	gc.run.lastReportedConsumption = make([]rmpb.ResourceDetail, typeLen)
	gc.run.resourceTokens = make(map[rmpb.ResourceType]*resourceTokenCounter)

	for typ := range allowResourceList {
		tb := getResourceTokenBucketFromSettings(gc.groupSettings, typ)
		if tb == nil {
			log.Error("error resource type", zap.String("type", rmpb.ResourceType_name[int32(typ)]))
			continue
		}
		//rate := Limit(tb.Settings.Fillrate)
		//brust := int(tb.Settings.BurstLimit)
		//tokens := tb.Tokens
		counter := &resourceTokenCounter{
			limiter: NewLimiter(0, initialRquestUnits, initialRquestUnits, gc.lowRUNotifyChan),
		}
		gc.run.resourceTokens[typ] = counter
	}

	go gc.groupLoop(controllerCtx)
	// TODO
	return gc
}

func (gc *groupCostController) OnRequestWait(
	ctx context.Context, info RequestInfo,
) error {
	delta := make(map[rmpb.ResourceType]*rmpb.ResourceDetail)
	for _, calc := range gc.calculators {
		calc.BeforeKVRequest(delta, info)
	}
	var wg sync.WaitGroup
	wg.Add(len(allowResourceList))
	var errReturn error
	for typ, counter := range gc.run.resourceTokens {
		go func() {
			v, ok := delta[typ]
			if ok {
				value := v.Value
				err := counter.limiter.WaitN(ctx, int(value))
				if err != nil {
					errReturn = err
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if errReturn != nil {
		return errReturn
	}

	gc.mu.Lock()
	for typ, detail := range delta {
		Add(&gc.mu.consumptions[typ], detail)
	}
	gc.mu.Unlock()
	return nil
}

func (gc *groupCostController) OnResponse(ctx context.Context, req RequestInfo, resp ResponseInfo) {
	delta := make(map[rmpb.ResourceType]*rmpb.ResourceDetail)
	for _, calc := range gc.calculators {
		calc.AfterKVRequest(delta, req, resp)
	}
	for typ, counter := range gc.run.resourceTokens {
		v, ok := delta[typ]
		if ok {
			value := v.Value
			counter.limiter.RemoveTokens(time.Now(), float64(value))
		}
	}
	gc.mu.Lock()
	for typ, detail := range delta {
		Add(&gc.mu.consumptions[typ], detail)
	}
	gc.mu.Unlock()
}

func (gc *groupCostController) initRunState(ctx context.Context) {
	gc.run.targetPeriod = gc.mainCfg.targetPeriod

	now := time.Now()
	gc.run.now = now
	gc.run.lastRequestTime = now
	for typ := range allowResourceList {
		counter := gc.run.resourceTokens[typ]
		// todo : it seems need to adjust algorithm
		// tb := getResourceTokenBucketFromSettings(gc.groupSettings, typ)
		// if tb == nil {
		// 	log.Error("error resource type", zap.String("type", rmpb.ResourceType_name[int32(typ)]))
		// 	continue
		// }
		counter.avgRUPerSec = initialRquestUnits / gc.run.targetPeriod.Seconds()
		counter.avgLastTime = now
	}
}

func (gc *groupCostController) updateRunState(ctx context.Context) {
	newTime := time.Now()
	delta := make(map[rmpb.ResourceType]*rmpb.ResourceDetail)
	for _, calc := range gc.calculators {
		calc.Trickle(delta, ctx)
	}
	gc.mu.Lock()
	for typ, detail := range delta {
		Add(&gc.mu.consumptions[typ], detail)
	}
	copy(gc.run.consumptions, gc.mu.consumptions)
	gc.mu.Unlock()
	log.Info("update run state", zap.Any("comsumption", gc.run.consumptions))
	gc.run.now = newTime
}

func (gc *groupCostController) updateAvgRUPerSec(ctx context.Context) {
	for typ := range allowResourceList {
		counter := gc.run.resourceTokens[typ]
		deltaDuration := gc.run.now.Sub(counter.avgLastTime)
		if deltaDuration <= 10*time.Millisecond {
			continue
		}
		delta := float64(gc.run.consumptions[typ].Value-counter.avgRUPerSecLastRU) / deltaDuration.Seconds()
		counter.avgRUPerSec = movingAvgFactor*counter.avgRUPerSec + (1-movingAvgFactor)*delta
		log.Info("[resource group controllor] update avg ru per sec", zap.Float64("avgRUPerSec", counter.avgRUPerSec))

	}
}

func (gc *groupCostController) shouldReportConsumption() bool {
	if gc.run.requestInProgress {
		return false
	}
	timeSinceLastRequest := gc.run.now.Sub(gc.run.lastRequestTime)
	if timeSinceLastRequest >= gc.run.targetPeriod {
		for typ := range allowResourceList {
			consumptionToReport := gc.run.consumptions[typ].Value - gc.run.lastReportedConsumption[typ].Value
			if consumptionToReport >= consumptionsReportingThreshold {
				return true
			}
			if timeSinceLastRequest >= extendedReportingPeriodFactor*gc.run.targetPeriod {
				return true
			}
		}
	}

	return false
}

func (gc *groupCostController) handleTokenBucketResponse(
	ctx context.Context, resp []*rmpb.GrantedTokenBucket) {
	if !gc.run.initialRequestCompleted {
		gc.run.initialRequestCompleted = true
		// This is the first successful request. Take back the initial RUs that we
		// used to pre-fill the bucket.
		for _, counter := range gc.run.resourceTokens {
			counter.limiter.RemoveTokens(gc.run.now, initialRquestUnits)
		}
	}

	for _, counter := range gc.run.resourceTokens {
		counter.limiter.RemoveTokens(gc.run.now, initialRquestUnits)
	}

	for _, grantedTB := range resp {
		typ := grantedTB.GetType()
		// todo: check whether grant = 0
		counter, ok := gc.run.resourceTokens[typ]
		if !ok {
			log.Warn("not support this resource type", zap.String("type", rmpb.ResourceType_name[int32(typ)]))
			continue
		}
		granted := grantedTB.GrantedTokens.Tokens
		if !counter.lastDeadline.IsZero() {
			// If last request came with a trickle duration, we may have RUs that were
			// not made available to the bucket yet; throw them together with the newly
			// granted RUs.
			if since := counter.lastDeadline.Sub(gc.run.now); since > 0 {
				granted += counter.lastRate * since.Seconds()
			}
		}
		if counter.setupNotificationTimer != nil {
			counter.setupNotificationTimer.Stop()
			counter.setupNotificationTimer = nil
			counter.setupNotificationCh = nil
		}
		notifyThreshold := granted * notifyFraction
		if notifyThreshold < bufferRUs {
			notifyThreshold = bufferRUs
		}

		var cfg tokenBucketReconfigureArgs
		if grantedTB.TrickleTimeMs == 0 {
			cfg.NewTokens = granted
			cfg.NewRate = 0
			cfg.NewBrust = int(granted + 1)
			cfg.NotifyThreshold = notifyThreshold
			counter.lastDeadline = time.Time{}
		} else {
			deadline := gc.run.now.Add(time.Duration(grantedTB.TrickleTimeMs) * time.Millisecond)
			cfg.NewRate = float64(grantedTB.GrantedTokens.Settings.Fillrate)

			timerDuration := grantedTB.TrickleTimeMs - 1000
			if timerDuration <= 0 {
				timerDuration = (grantedTB.TrickleTimeMs + 1000) / 2
			}
			counter.setupNotificationTimer = time.NewTimer(time.Duration(timerDuration) * time.Millisecond)
			counter.setupNotificationCh = counter.setupNotificationTimer.C
			counter.setupNotificationThreshold = notifyThreshold

			counter.lastDeadline = deadline
		}
		counter.lastRate = cfg.NewRate
		counter.limiter.Reconfigure(gc.run.now, cfg)
	}
}

func (gc *groupCostController) sendTokenBucketRequest(ctx context.Context, source string) {
	deltaConsumption := make([]rmpb.ResourceDetail, typeLen)
	for typ, cons := range gc.run.consumptions {
		deltaConsumption[typ] = cons
		Sub(&deltaConsumption[typ], &gc.run.lastReportedConsumption[typ])
	}

	requests := make([]*rmpb.ResourceDetail, 0)
	for typ := range allowResourceList {
		counter := gc.run.resourceTokens[typ]
		var value float64 = initialRquestUnits
		if gc.run.initialRequestCompleted {
			value = counter.avgRUPerSec*gc.run.targetPeriod.Seconds() + bufferRUs
			value -= float64(counter.limiter.AvailableTokens(gc.run.now))
			if value < 0 {
				value = 0
			}
		}
		request := &rmpb.ResourceDetail{
			Type:  typ,
			Value: uint64(value),
		}
		requests = append(requests, request)
	}

	now := time.Now()
	log.Info("[tenant controllor] send token bucket request", zap.Time("now", now), zap.Any("req", requests), zap.String("source", source), zap.String("resource group", gc.resourceGroupName))
	gc.run.lastRequestTime = gc.run.now
	copy(gc.run.lastReportedConsumption, gc.run.consumptions)
	gc.run.requestInProgress = true
	go func() {
		resp, err := gc.provider.AcquireTokenBuckets(ctx, gc.resourceGroupName, uint64(gc.run.targetPeriod.Milliseconds()), requests, deltaConsumption)
		if err != nil {
			// Don't log any errors caused by the stopper canceling the context.
			if !errors.ErrorEqual(err, context.Canceled) {
				log.L().Sugar().Infof("TokenBucket RPC error: %v", err)
			}
			resp = nil
		}
		log.Info("[tenant controllor] token bucket response", zap.Time("now", time.Now()), zap.Any("resp", resp), zap.String("source", source), zap.Duration("latency", time.Since(now)))
		gc.responseChan <- resp
	}()
}

func (gc *groupCostController) groupLoop(ctx context.Context) {
	interval := gc.mainCfg.groupLoopUpdateInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	gc.initRunState(ctx)
	gc.updateRunState(ctx)
	gc.sendTokenBucketRequest(ctx, "init")

	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-gc.responseChan:
			gc.run.requestInProgress = false
			if resp != nil {
				gc.updateRunState(ctx)
				gc.handleTokenBucketResponse(ctx, resp)
			} else {
				// A nil response indicates a failure (which would have been logged).
				gc.run.requestNeedsRetry = true
			}
		case <-ticker.C:
			gc.updateRunState(ctx)
			gc.updateAvgRUPerSec(ctx)
			if gc.run.requestNeedsRetry || gc.shouldReportConsumption() {
				gc.run.requestNeedsRetry = false
				gc.sendTokenBucketRequest(ctx, "report")
			}
		case <-gc.lowRUNotifyChan:
			gc.updateRunState(ctx)
			if !gc.run.requestInProgress {
				gc.sendTokenBucketRequest(ctx, "low_ru")
			}
		}
		for _, counter := range gc.run.resourceTokens {
			select {
			case <-counter.setupNotificationCh:
				counter.setupNotificationTimer = nil
				counter.setupNotificationCh = nil

				gc.updateRunState(ctx)
				counter.limiter.SetupNotification(gc.run.now, float64(counter.setupNotificationThreshold))
			default:
				continue
			}
		}
	}
}
