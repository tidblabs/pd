// Copyright 2019 TiKV Project Authors.
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

package operator

import (
	"time"

	"github.com/tikv/pd/pkg/syncutil"
)

// Only record non-end status and one end status.
type statusTimes [firstEndStatus + 1]time.Time

// OpStatusTracker represents the status of an operator.
type OpStatusTracker struct {
	rw         syncutil.RWMutex
	current    OpStatus    // Current status
	reachTimes statusTimes // Time when reach the current status
}

// NewOpStatusTracker creates an OpStatus.
func NewOpStatusTracker() OpStatusTracker {
	return OpStatusTracker{
		current:    CREATED,
		reachTimes: statusTimes{CREATED: time.Now()},
	}
}

// Status returns current status.
func (trk *OpStatusTracker) Status() OpStatus {
	trk.rw.RLock()
	defer trk.rw.RUnlock()
	return trk.current
}

// ReachTime returns the reach time of current status.
func (trk *OpStatusTracker) ReachTime() time.Time {
	trk.rw.RLock()
	defer trk.rw.RUnlock()
	return trk.getTime(trk.current)
}

// ReachTimeOf returns the time when reached given status. If didn't reached the given status, return zero.
func (trk *OpStatusTracker) ReachTimeOf(s OpStatus) time.Time {
	trk.rw.RLock()
	defer trk.rw.RUnlock()
	return trk.getTime(s)
}

func (trk *OpStatusTracker) getTime(s OpStatus) time.Time {
	if s < firstEndStatus {
		return trk.reachTimes[s]
	} else if trk.current == s {
		return trk.reachTimes[firstEndStatus]
	} else {
		return time.Time{}
	}
}

// To transfer the current status to dst if this transition is valid,
// returns whether transferred.
func (trk *OpStatusTracker) To(dst OpStatus) bool {
	trk.rw.Lock()
	defer trk.rw.Unlock()
	return trk.toLocked(dst)
}

func (trk *OpStatusTracker) toLocked(dst OpStatus) bool {
	if dst < statusCount && validTrans[trk.current][dst] {
		trk.current = dst
		trk.setTime(trk.current, time.Now())
		return true
	}
	return false
}

func (trk *OpStatusTracker) setTime(st OpStatus, t time.Time) {
	if st < firstEndStatus {
		trk.reachTimes[st] = t
	} else {
		trk.reachTimes[firstEndStatus] = t
	}
}

// IsEnd checks whether the current status is an end status.
func (trk *OpStatusTracker) IsEnd() bool {
	trk.rw.RLock()
	defer trk.rw.RUnlock()
	return IsEndStatus(trk.current)
}

// CheckExpired checks if expired, and update the current status.
func (trk *OpStatusTracker) CheckExpired(exp time.Duration) bool {
	trk.rw.Lock()
	defer trk.rw.Unlock()
	if trk.current == CREATED {
		if time.Since(trk.reachTimes[CREATED]) < exp {
			return false
		}
		_ = trk.toLocked(EXPIRED)
		return true
	}
	return trk.current == EXPIRED
}

// CheckStepTimeout checks if timeout, and update the current status.
func (trk *OpStatusTracker) CheckStepTimeout(start time.Time, step OpStep, approximateSize int64) bool {
	trk.rw.Lock()
	defer trk.rw.Unlock()
	if trk.current == STARTED {
		if !step.Timeout(start, approximateSize) {
			return false
		}
		_ = trk.toLocked(TIMEOUT)
		return true
	}
	return trk.current == TIMEOUT
}

// String implements fmt.Stringer.
func (trk *OpStatusTracker) String() string {
	trk.rw.RLock()
	defer trk.rw.RUnlock()
	return OpStatusToString(trk.current)
}
