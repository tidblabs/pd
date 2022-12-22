// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"math"
	"time"

	"github.com/gogo/protobuf/proto"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const defaultRefillRate = 10000

const defaultInitialTokens = 10 * 10000

// GroupTokenBucket is a token bucket for a resource group.
type GroupTokenBucket struct {
	*rmpb.TokenBucket `json:"token_bucket,omitempty"`
	Consumption       *rmpb.TokenBucketsRequest `json:"consumption,omitempty"`
	LastUpdate        *time.Time                `json:"last_update,omitempty"`
	Initialized       bool                      `json:"initialized"`
}

// patch patches the token bucket settings.
func (t *GroupTokenBucket) patch(settings *rmpb.TokenBucket) {
	if settings == nil {
		return
	}
	tb := proto.Clone(t.TokenBucket).(*rmpb.TokenBucket)
	if settings.GetSettings() != nil {
		if tb == nil {
			tb = &rmpb.TokenBucket{}
		}
		tb.Settings = settings.GetSettings()
	}

	// the settings in token is delta of the last update and now.
	tb.Tokens += settings.GetTokens()
	t.TokenBucket = tb
}

// Update updates the token bucket.
func (t *GroupTokenBucket) update(now time.Time) {
	if !t.Initialized {
		t.Settings.Fillrate = defaultRefillRate
		t.Tokens = defaultInitialTokens
		t.LastUpdate = &now
		t.Initialized = true
		return
	}

	delta := now.Sub(*t.LastUpdate)
	if delta > 0 {
		t.Tokens += float64(t.Settings.Fillrate) * delta.Seconds()
		t.LastUpdate = &now
	}
}

// Request requests tokens from the token bucket.
func (t *GroupTokenBucket) request(
	neededTokens float64, targetPeriodMs uint64,
) *rmpb.TokenBucket {
	var res rmpb.TokenBucket
	res.Settings = &rmpb.TokenLimitSettings{}
	// TODO: consider the shares for dispatch the fill rate
	res.Settings.Fillrate = t.Settings.Fillrate

	if neededTokens <= 0 {
		return &res
	}

	if t.Tokens >= neededTokens {
		t.Tokens -= neededTokens
		// granted the total request tokens
		res.Tokens = neededTokens
		return &res
	}

	var grantedTokens float64
	if t.Tokens > 0 {
		grantedTokens = t.Tokens
		neededTokens -= grantedTokens
	}

	availableRate := float64(t.Settings.Fillrate)
	if debt := -t.Tokens; debt > 0 {
		debt -= float64(t.Settings.Fillrate) * float64(targetPeriodMs) / 1000
		if debt > 0 {
			debtRate := debt / float64(targetPeriodMs/1000)
			availableRate -= debtRate
			availableRate = math.Max(availableRate, 0.05*t.Tokens)
		}
	}

	consumptionDuration := time.Duration(float64(time.Second) * (neededTokens / availableRate))
	targetDuration := time.Duration(targetPeriodMs/1000) * time.Second
	if consumptionDuration <= targetDuration {
		grantedTokens += neededTokens
	} else {
		grantedTokens += availableRate * targetDuration.Seconds()
	}
	t.Tokens -= grantedTokens
	res.Settings.Fillrate = uint64(availableRate)
	res.Tokens = grantedTokens
	return &res
}
