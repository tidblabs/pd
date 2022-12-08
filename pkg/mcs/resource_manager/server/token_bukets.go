// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"math"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const defaultRefillRate = 10000

const defaultInitialRUs = 10 * 10000

type GroupTokenBucket struct {
	LastUpdate  time.Time                 `json:"last_update"`
	TokenBucket TokenBucket               `json:"token_bucket"`
	Initialized bool                      `json:"initialized"`
	Consumption *rmpb.TokenBucketsRequest `json:"consumption"`
}

func (t *GroupTokenBucket) Update(now time.Time) {
	if !t.Initialized {
		t.TokenBucket.Settings.Fillrate = defaultRefillRate
		t.TokenBucket.Tokens = defaultInitialRUs
		t.LastUpdate = now
		t.Initialized = true
		return
	}

	delta := now.Sub(t.LastUpdate)
	if delta > 0 {
		t.TokenBucket.Update(delta)
		t.LastUpdate = now
	}
}

type TokenBucket struct {
	Id string `json:"id"`
	*rmpb.TokenBucket
}

func (s *TokenBucket) Update(sinceDuration time.Duration) {
	if sinceDuration > 0 {
		s.Tokens += float64(s.Settings.Fillrate) * sinceDuration.Seconds()
	}
}

func (s *TokenBucket) Request(
	ctx context.Context, neededTokens float64, targetPeriodMs int64,
) *rmpb.TokenBucket {
	var res rmpb.TokenBucket
	// TODO: consider the shares for dispatch the fill rate
	res.Settings.Fillrate = s.Settings.Fillrate
	log.Debug("token bucket request", zap.String("id", s.Id), zap.Float64("current-tokens", s.Tokens), zap.Uint64("fill-rate", s.Settings.Fillrate), zap.Float64("requested-tokens", neededTokens))

	if neededTokens <= 0 {
		return &res
	}

	if s.Tokens >= neededTokens {
		s.Tokens -= neededTokens
		// granted the total request tokens
		res.Tokens = neededTokens
		return &res
	}

	var grantedTokens float64
	if s.Tokens > 0 {
		grantedTokens = s.Tokens
		neededTokens -= grantedTokens
	}

	availableRate := float64(s.Settings.Fillrate)
	if debt := -s.Tokens; debt > 0 {
		debt -= float64(s.Settings.Fillrate) * float64(targetPeriodMs) / 1000
		if debt > 0 {
			debtRate := debt / float64(targetPeriodMs/1000)
			availableRate -= debtRate
			availableRate = math.Max(availableRate, 0.05*s.Tokens)
		}
	}

	consumptionDuration := time.Duration(float64(time.Second) * (neededTokens / availableRate))
	targetDuration := time.Duration(targetPeriodMs/1000) * time.Second
	if consumptionDuration <= targetDuration {
		grantedTokens += neededTokens
	} else {
		grantedTokens += availableRate * targetDuration.Seconds()
	}
	s.Tokens -= grantedTokens
	res.Settings.Fillrate = uint64(availableRate)
	res.Tokens = grantedTokens
	log.Debug("request granted over time ", zap.String("id", s.Id), zap.Float64("current-tokens", s.Tokens), zap.Float64("granted-tokens", res.Tokens), zap.Uint64("granted-fill-rate", res.Settings.Fillrate), zap.Float64("requested-tokens", neededTokens))

	return &res
}
