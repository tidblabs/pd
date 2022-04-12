package endpoint

import (
	"encoding/json"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/tenant"
)

type TenantStorage interface {
	LoadTenantTokenBucket(tenantID uint64) (*tenant.TenantTokenBucket, error)
	SaveTenantTokenBucket(tenantID uint64, bucket *tenant.TenantTokenBucket) error
}

var _ RuleStorage = (*StorageEndpoint)(nil)

// SaveTenantTokenBucket stores a tenant bucket cfg to the tenantPath.
func (se *StorageEndpoint) SaveTenantTokenBucket(tenantID uint64, bucket *tenant.TenantTokenBucket) error {

	value, err := json.Marshal(bucket)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return se.Save(TenanTokenBucketPath(tenantID), string(value))
}

// LoadTenantTokenBucket loads a tenant bucket cfg from the tenantPath.
func (se *StorageEndpoint) LoadTenantTokenBucket(tenantID uint64) (*tenant.TenantTokenBucket, error) {
	value, err := se.Load(TenanTokenBucketPath(tenantID))
	if err != nil {
		return nil, err
	}
	var bucket tenant.TenantTokenBucket
	if len(value) == 0 {
		return &bucket, nil
	}

	err = json.Unmarshal([]byte(value), &bucket)
	if err != nil {
		return nil, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	return &bucket, nil
}
