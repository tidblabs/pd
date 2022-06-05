// Copyright 2018 TiKV Project Authors.
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

package api

import (
	"fmt"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/errcode"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

type tenantHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newTenantHandler(svr *server.Server, rd *render.Render) *tenantHandler {
	return &tenantHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags tenant
// @Summary Get the Tenant Token Config.
// @Param id path integer true "Tenant Id"
// @Produce json
// @Success 200 {object} tenant.TenantTokenBucket
// @Router /tenant/{id}/token_bucket [get]
func (h *tenantHandler) GetTokenBucket(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	tenantID, err := apiutil.ParseUint64VarsField(vars, "id")
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err1 := rc.GetStorage().LoadTenantTokenBucket(tenantID)
	if err1 != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, res)
}

// @Tags tenant
// @Summary Update a config item.
// @Accept json
// @Produce json
// @Success 200 {string} string "The config is updated."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /tenant/{id}/token_bucket [post]
func (h *tenantHandler) SetTokenBucket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tenantID, errParse := apiutil.ParseUint64VarsField(vars, "id")
	if errParse != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errParse))
		return
	}

	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	rc := getCluster(r)
	tenantInfo, err1 := rc.GetStorage().LoadTenantTokenBucket(tenantID)
	if err1 != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	bucketState := tenantInfo.Bucket
	updated, _, err := mergeConfig(&bucketState, data)
	if err1 != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	tenantInfo.Bucket = bucketState
	log.Info("update tenant token bucket", zap.Any("tenant", tenantInfo))
	err = rc.GetStorage().SaveTenantTokenBucket(tenantID, tenantInfo)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, fmt.Sprintf("updated(changed=%v) bucket settings successfully.", updated))
}
