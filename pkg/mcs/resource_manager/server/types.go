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

// Package types provides a set of struct definitions for the resource group, can be imported.
package server

import (
	"encoding/json"
	"errors"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tipb/go-tipb"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ResourceGroup is the definition of a resource group, for REST API.
type ResourceGroup struct {
	ID               int64             `json:"id"`
	Name             string            `json:"name"`
	RRU_PER_SEC      uint64            `json:"rru_per_sec"`
	WRU_PER_SEC      uint64            `json:"wru_per_sec"`
	CPU              resource.Quantity `json:"cpu"`
	IOReadBandwidth  resource.Quantity `json:"io_read_bandwidth"`
	IOWriteBandwidth resource.Quantity `json:"io_write_bandwidth"`
}

// Validate validates the resource group.
func (r *ResourceGroup) Validate() error {
	if r.IOReadBandwidth.IsZero() && r.IOWriteBandwidth.IsZero() {
		return errors.New("resource group is invalid, need set io quota")
	}
	if r.IOReadBandwidth.IsZero() || r.IOWriteBandwidth.IsZero() {
		return errors.New("resource group is invalid, need set io read/write quota both")
	}
	return nil
}

// IntoNodeConfig converts a ResourceGroupSpec to a ResourceGroup.
func (r *ResourceGroup) IntoNodeConfig(num int) *NodeResourceGroup {
	var read, write int64
	read = r.IOReadBandwidth.Value() / int64(num)
	write = r.IOWriteBandwidth.Value() / int64(num)

	return &NodeResourceGroup{
		ID:               r.ID,
		Name:             r.Name,
		CPU:              float64(r.CPU.MilliValue()) / float64(num),
		IOReadBandwidth:  read,
		IOWriteBandwidth: write,
	}
}

func (rg *ResourceGroup) IntoProto() *rmpb.ResourceGroup {
	group := &rmpb.ResourceGroup{
		Settings: &rmpb.GroupSettings{
			RRU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{Fillrate: rg.RRU_PER_SEC},
			},
			WRU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{Fillrate: rg.WRU_PER_SEC},
			},
			ReadBandwidth: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{Fillrate: uint64(rg.IOReadBandwidth.Value())},
			},
			WriteBandwidth: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{Fillrate: uint64(rg.IOReadBandwidth.Value())},
			},
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

func DecodeResourceTag(tagBytes []byte) *tipb.ResourceGroupTag {
	tag := &tipb.ResourceGroupTag{}
	if err := tag.Unmarshal(tagBytes); err != nil {
		panic(err)
	}
	return tag
}
