// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package server provides a set of struct definitions for the resource group, can be imported.
package server

import (
	"encoding/json"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

// ResourceGroup is the definition of a resource group, for REST API.
type ResourceGroup struct {
	Name             string           `json:"name"`
	RRU              GroupTokenBucket `json:"rru"`
	WRU              GroupTokenBucket `json:"wru"`
	CPU              GroupTokenBucket `json:"cpu,omitempty"`
	IOReadBandwidth  GroupTokenBucket `json:"io_read_bandwidth,omitempty"`
	IOWriteBandwidth GroupTokenBucket `json:"io_write_bandwidth,omitempty"`
}

// FromProtoResourceGroup converts a rmpb.ResourceGroup to a ResourceGroup.
func FromProtoResourceGroup(group *rmpb.ResourceGroup) *ResourceGroup {
	rg := &ResourceGroup{
		Name: group.ResourceGroupName,
		RRU: GroupTokenBucket{
			TokenBucketState: TokenBucket{
				TokenBucket: group.Settings.RRU,
			},
		},
		WRU: GroupTokenBucket{
			TokenBucketState: TokenBucket{
				TokenBucket: group.Settings.WRU,
			},
		},
		IOReadBandwidth: GroupTokenBucket{
			TokenBucketState: TokenBucket{
				TokenBucket: group.Settings.ReadBandwidth,
			},
		},
		IOWriteBandwidth: GroupTokenBucket{
			TokenBucketState: TokenBucket{
				TokenBucket: group.Settings.WriteBandwidth,
			},
		},
	}
	return rg
}

func (rg *ResourceGroup) Copy() *ResourceGroup {
	res, err := json.Marshal(rg)
	if err != nil {
		panic(err)
	}
	var newRg ResourceGroup
	err = json.Unmarshal(res, &newRg)
	if err != nil {
		panic(err)
	}
	return &newRg
}

// IntoNodeResourceGroup converts a ResourceGroup to a NodeResourceGroup.
func (rg *ResourceGroup) IntoNodeResourceGroup(num int) *NodeResourceGroup {
	return &NodeResourceGroup{
		Name: rg.Name,
		CPU:  float64(rg.CPU.GetTokenBucket().Settings.Fillrate) / float64(num),
	}
}

// IntoProtoResourceGroup converts a ResourceGroup to a rmpb.ResourceGroup.
func (rg *ResourceGroup) IntoProtoResourceGroup() *rmpb.ResourceGroup {
	group := &rmpb.ResourceGroup{
		ResourceGroupName: rg.Name,
		Settings: &rmpb.GroupSettings{
			RRU:            rg.RRU.GetTokenBucket(),
			WRU:            rg.WRU.GetTokenBucket(),
			ReadBandwidth:  rg.IOReadBandwidth.GetTokenBucket(),
			WriteBandwidth: rg.IOWriteBandwidth.GetTokenBucket(),
		},
	}
	return group
}

// NodeResourceGroup is the definition of a resource group, for REST API.
type NodeResourceGroup struct {
	ID               int64   `json:"id"`
	Name             string  `json:"name"`
	CPU              float64 `json:"cpu-quota"`
	IOReadBandwidth  int64   `json:"read-bandwidth"`
	IOWriteBandwidth int64   `json:"write-bandwidth"`
}

// ToJSON converts a NodeResourceGroup to a JSON string.
func (r *NodeResourceGroup) ToJSON() []byte {
	res, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return res
}
