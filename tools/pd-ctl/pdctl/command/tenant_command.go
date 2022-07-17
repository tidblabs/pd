// Copyright 2022 TiKV Project Authors.
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

package command

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var (
	tenantBucketPrefix = "pd/api/v1/tenant/%v/token_bucket"
)

// NewTenantCommand return a config subcommand of rootCmd
func NewTenantCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "tenant <subcommand>",
		Short: "tune pd configs",
	}
	conf.AddCommand(NewShowTenantCommand())
	conf.AddCommand(NewSetTenantCommand())
	return conf
}

// NewShowTenantCommand return a show subcommand of configCmd
func NewShowTenantCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "show <tenant_id>",
		Short: "show tenant config",
		Run:   showTenantConfigCommandFunc,
	}
	return sc
}

func showTenantConfigCommandFunc(cmd *cobra.Command, args []string) {
	argsCount := len(args)
	if argsCount != 1 {
		cmd.Usage()
		return
	}
	prefix := tenantBucketPrefix
	path := fmt.Sprintf(prefix, args[0])
	r, err := doRequest(cmd, path, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get tenant config: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewSetTenantCommand return a show subcommand of configCmd
func NewSetTenantCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "set <tenant_id> [<key> <value>]",
		Short: "set tenant config",
		Run:   setTenantConfigCommandFunc,
	}
	return sc
}

func setTenantConfigCommandFunc(cmd *cobra.Command, args []string) {
	argsCount := len(args)
	if argsCount != 3 {
		fmt.Println(args)
		cmd.Usage()
		return
	}
	prefix := tenantBucketPrefix
	path := fmt.Sprintf(prefix, args[0])
	opt, val := args[1], args[2]
	err := postConfigDataWithPath(cmd, opt, val, path)
	if err != nil {
		cmd.Printf("Failed to set tenant: %s\n", err)
		return
	}
	cmd.Println("Success!")
}
