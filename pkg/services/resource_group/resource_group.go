package resourcegroup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/errcode"
	"github.com/pingcap/log"
	cors "github.com/rs/cors/wrapper/gin"
	"github.com/tikv/pd/pkg/services/resource_group/types"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage"
	"go.uber.org/zap"
)

// APIPathPrefix is the prefix of the API path.
const APIPathPrefix = "/resource-groups/api/v1alpha/"

var (
	apiServiceGroup = server.ServiceGroup{
		Name:       "resource-groups",
		Version:    "v1alpha",
		IsCore:     false,
		PathPrefix: APIPathPrefix,
	}
)

// GetServiceBuilders returns all ServiceBuilders required by Dashboard
func GetServiceBuilders() []server.HandlerBuilder {
	//	var err error

	// The order of execution must be sequential.
	return []server.HandlerBuilder{
		// Dashboard API Service
		func(ctx context.Context, srv *server.Server) (http.Handler, server.ServiceGroup, error) {
			s := NewService(srv)
			srv.AddStartCallback(s.Start)
			srv.AddCloseCallback(s.Stop)
			return s.handler(), apiServiceGroup, nil
		},
	}
}

// Service is the resource group service.
type Service struct {
	apiHandlerEngine *gin.Engine
	baseEndpoint     *gin.RouterGroup

	manager *Manager
}

// NewService returns a new Service.
func NewService(srv *server.Server) *Service {
	apiHandlerEngine := gin.New()
	apiHandlerEngine.Use(gin.Recovery())
	apiHandlerEngine.Use(cors.AllowAll())
	apiHandlerEngine.Use(gzip.Gzip(gzip.DefaultCompression))
	endpoint := apiHandlerEngine.Group(APIPathPrefix)
	s := &Service{
		manager:          NewManager(srv),
		apiHandlerEngine: apiHandlerEngine,
		baseEndpoint:     endpoint,
	}
	s.RegisterRouter()
	return s
}

// RegisterRouter registers the router of the service.
func (s *Service) RegisterRouter() {
	configEndpoint := s.baseEndpoint.Group("/config")
	configEndpoint.POST("/group", s.putResourceGroup)
	configEndpoint.GET("/group/:name", s.getResourceGroup)
	configEndpoint.GET("/groups", s.getResourceGroupList)
	configEndpoint.DELETE("/group/:name", s.deleteResourceGroup)
}

// Start starts the Manager.
func (s *Service) Start() {
	s.manager.Init()
}

// Stop starts the Manager.
func (s *Service) Stop() {
	s.manager.Close()
}

func (s *Service) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.apiHandlerEngine.ServeHTTP(w, r)
	})
}

// @Summary add a resource group
// @Param address path string true "ip:port"
// @Success 200 "added successfully"
// @Failure 401 {object} rest.ErrorResponse
// @Router /config/group/ [post]
func (s *Service) putResourceGroup(c *gin.Context) {
	var group types.ResourceGroup
	if err := c.ShouldBindJSON(&group); err != nil {
		log.Error("bind json error", zap.Error(err))
		c.JSON(http.StatusBadRequest, err.Error())
		return
	}
	if err := s.manager.PutResourceGroup(&group); err != nil {
		log.Error("put resource group error", zap.Error(err))
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, "success")
}

// @ID getResourceGroup
// @Summary Get current alert count from AlertManager
// @Success 200 {object} int
// @Param name string true "groupName"
// @Router /config/group/{name} [get]
// @Failure 404 {object} rest.ErrorResponse
func (s *Service) getResourceGroup(c *gin.Context) {
	group := s.manager.GetResourceGroup(c.Param("name"))
	if group == nil {
		c.JSON(http.StatusNotFound, errcode.NewNotFoundErr(errors.New("resource group not found")))
	}
	c.JSON(http.StatusOK, group)
}

// @ID getResourceGroupList
// @Summary Get current alert count from AlertManager
// @Success 200 {array} types.ResourceGroup
// @Router /config/groups [get]
func (s *Service) getResourceGroupList(c *gin.Context) {
	groups := s.manager.GetResourceGroupList()
	c.JSON(http.StatusOK, groups)
}

// @ID getResourceGroup
// @Summary Get current alert count from AlertManager
// @Success 200 "deleted successfully"
// @Param name string true "groupName"
// @Router /config/group/{name} [get]
// @Failure 404 {object} error
func (s *Service) deleteResourceGroup(c *gin.Context) {
	//TODO: Implement deleteResourceGroup
}

// Manager is the manager of resource group.
type Manager struct {
	ctx context.Context
	sync.RWMutex
	running bool
	wg      sync.WaitGroup
	groups  map[string]*types.ResourceGroup
	storage func() storage.Storage
	// TODO: dispatch resource group to storage node
	getStores func() ([]*core.StoreInfo, error)
}

// NewManager returns a new Manager.
func NewManager(srv *server.Server) *Manager {
	getStores := func() ([]*core.StoreInfo, error) {
		rc := srv.GetRaftCluster()
		if rc == nil {
			return nil, errors.New("RaftCluster is nil")
		}
		return rc.GetStores(), nil
	}

	m := &Manager{
		ctx:       srv.Context(),
		groups:    make(map[string]*types.ResourceGroup),
		storage:   srv.GetStorage,
		getStores: getStores,
	}
	return m
}

// Init initializes the resource group manager.
func (m *Manager) Init() {
	handler := func(k, v string) {
		var group types.ResourceGroup
		if err := json.Unmarshal([]byte(v), &group); err != nil {
			panic(err)
		}
		m.groups[group.Name] = &group
	}
	m.storage().LoadResourceGroups(handler)
	m.wg.Add(1)
	go m.dispatchResourceGroupToNodes()
	m.Lock()
	m.running = true
	m.Unlock()
}

// Close closes the resource group manager.
func (m *Manager) Close() {
	m.wg.Wait()
	log.Info("resource group manager closed")
}

func (m *Manager) dispatchResourceGroupToNodes() error {
	defer m.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-m.ctx.Done():
			log.Info("dispatchResourceGroupToNodes is stopped")
			return nil
		case <-ticker.C:
		}

		stores, err := m.getStores()
		if err != nil {
			return err
		}
		storeNums := 0
		for _, store := range stores {
			if store.IsUp() || store.IsRemoving() {
				storeNums++
			}
		}
		storeGroups := make([]*types.NodeResourceGroup, 0, storeNums)
		m.RLock()
		for _, group := range m.groups {
			storeGroups = append(storeGroups, group.IntoNodeConfig(storeNums))
		}
		m.RUnlock()
		for _, store := range stores {
			if store.IsUp() || store.IsRemoving() {
				_, targetStatuAddr := ResolveLoopBackAddr(store.GetAddress(), store.GetMeta().GetStatusAddress())
				for _, group := range storeGroups {
					// FIXME: https request
					resp, err := http.Post(fmt.Sprintf("http://%s/resource_group", targetStatuAddr), "application/json", bytes.NewBuffer(group.ToJSON()))
					body, _ := ioutil.ReadAll(resp.Body)
					if err != nil {
						log.Error("dispatch resource group to node", zap.String("addr", targetStatuAddr), zap.Error(err))
					} else {
						log.Debug("dispatch resource group to node", zap.String("addr", targetStatuAddr), zap.String("group", group.Name), zap.Int("status", resp.StatusCode), zap.String("group", string(group.ToJSON())), zap.String("body", string(body)))
					}
					resp.Body.Close()
				}
			}
		}
	}
}

func (m *Manager) isRunning() bool {
	m.RLock()
	defer m.RUnlock()
	return m.running
}

// PutResourceGroup puts a resource group.
func (m *Manager) PutResourceGroup(group *types.ResourceGroup) error {
	if !m.isRunning() {
		return errors.New("resource group manager is not running")
	}
	if err := group.Validate(); err != nil {
		return err
	}
	if err := m.storage().SaveResourceGroup(group.Name, group); err != nil {
		return err
	}
	m.Lock()
	m.groups[group.Name] = group
	m.Unlock()
	return nil
}

// DeleteResourceGroup deletes a resource group.
func (m *Manager) DeleteResourceGroup(name string) error {
	if !m.isRunning() {
		return errors.New("resource group manager is not running")
	}
	if err := m.storage().DeleteResourceGroup(name); err != nil {
		return err
	}
	m.Lock()
	delete(m.groups, name)
	m.Unlock()
	return nil
}

// GetResourceGroup returns a resource group.
func (m *Manager) GetResourceGroup(name string) *types.ResourceGroup {
	if !m.isRunning() {
		return nil
	}
	m.RLock()
	defer m.RUnlock()
	if group, ok := m.groups[name]; ok {
		return group
	}
	return nil
}

// GetResourceGroupList returns a resource group list.
func (m *Manager) GetResourceGroupList() []*types.ResourceGroup {
	if !m.isRunning() {
		return nil
	}
	res := make([]*types.ResourceGroup, 0)
	m.RLock()
	for _, group := range m.groups {
		res = append(res, group)
	}
	m.RUnlock()
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})
	return res
}

/// Utils

// isLoopBackOrUnspecifiedAddr checks if the address is loopback or unspecified.
func isLoopBackOrUnspecifiedAddr(addr string) bool {
	tcpAddr, err := net.ResolveTCPAddr("", addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(tcpAddr.IP.String())
	return ip != nil && (ip.IsUnspecified() || ip.IsLoopback())
}

// ResolveLoopBackAddr exports for testing.
func ResolveLoopBackAddr(peerAddr, statusAddr string) (newPeerAddr string, newStatusAddr string) {
	newPeerAddr, newStatusAddr = peerAddr, statusAddr
	if isLoopBackOrUnspecifiedAddr(peerAddr) && !isLoopBackOrUnspecifiedAddr(statusAddr) {
		addr, err1 := net.ResolveTCPAddr("", peerAddr)
		statusAddr, err2 := net.ResolveTCPAddr("", statusAddr)
		if err1 == nil && err2 == nil {
			addr.IP = statusAddr.IP
			newPeerAddr = addr.String()
		}
	} else if !isLoopBackOrUnspecifiedAddr(peerAddr) && isLoopBackOrUnspecifiedAddr(statusAddr) {
		addr, err1 := net.ResolveTCPAddr("", peerAddr)
		statusAddr, err2 := net.ResolveTCPAddr("", statusAddr)
		if err1 == nil && err2 == nil {
			statusAddr.IP = addr.IP
			newStatusAddr = statusAddr.String()
		}
	}
	return
}
