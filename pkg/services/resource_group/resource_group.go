package resourcegroup

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"sync"

	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/errcode"
	cors "github.com/rs/cors/wrapper/gin"
	"github.com/tikv/pd/pkg/services/resource_group/types"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage"
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
		c.JSON(http.StatusBadRequest, err)
		return
	}
	if err := s.manager.PutResourceGroup(&group); err != nil {
		c.JSON(http.StatusInternalServerError, err)
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
	sync.RWMutex
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
}

// PutResourceGroup puts a resource group.
func (m *Manager) PutResourceGroup(group *types.ResourceGroup) error {
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
	m.RLock()
	defer m.RUnlock()
	if group, ok := m.groups[name]; ok {
		return group
	}
	return nil
}

// GetResourceGroupList returns a resource group list.
func (m *Manager) GetResourceGroupList() []*types.ResourceGroup {
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
