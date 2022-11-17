package types

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestSizeJSON(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	b := &ResourceGroup{
		Name:        "test",
		CPU:         resource.MustParse("8000m"),
		IOBandwidth: resource.MustParse("1000Mi"),
	}
	o, err := json.Marshal(b)
	fmt.Println(string(o))
	re.NoError(err)

	var nb ResourceGroup
	err = json.Unmarshal(o, &nb)
	re.NoError(err)
	re.Equal(b.CPU.MilliValue(), nb.CPU.MilliValue())
	re.Equal(b.IOBandwidth.MilliValue(), nb.IOBandwidth.Value())
}

func TestValidAndCovert(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	testCase := []struct {
		src     string
		success bool
		target  string
	}{
		{
			`{"name":"test","cpu":"8","io_bandwidth":"1000Mi","io_read_bandwidth":"0","io_write_bandwidth":"0"}`,
			true,
			`{"name":"test","cpu_quota":8000,"io_read_bandwidth":1048576000,"io_write_bandwidth":1048576000}`,
		},
		{
			`{"name":"test","io_bandwidth":"1000Mi","io_read_bandwidth":"0","io_write_bandwidth":"0"}`,
			false,
			`{"name":"test","cpu_quota":0,"io_read_bandwidth":1048576000,"io_write_bandwidth":1048576000}`,
		},
		{
			`{"name":"test","cpu":"8","io_bandwidth":"0","io_read_bandwidth":"1000Mi","io_write_bandwidth":"0"}`,
			false,
			`{"name":"test","cpu_quota":8000,"io_read_bandwidth":1048576000,"io_write_bandwidth":0}`,
		},
		{
			`{"name":"test","cpu":"8", "io_bandwidth":"0","io_read_bandwidth":"0","io_write_bandwidth":"1000Mi"}`,
			false,
			`{"name":"test","cpu_quota":8000,"io_read_bandwidth":0,"io_write_bandwidth":1048576000}`,
		},
		{
			`{"name":"test", "cpu":"8", "io_bandwidth":"0","io_read_bandwidth":"1000Mi","io_write_bandwidth":"1000Mi"}`,
			true,
			`{"name":"test","cpu_quota":8000,"io_read_bandwidth":1048576000,"io_write_bandwidth":1048576000}`,
		},
		{
			`{"name":"test", "cpu":"8000m", "io_bandwidth":"0","io_read_bandwidth":"1000Mi","io_write_bandwidth":"1000Mi"}`,
			true,
			`{"name":"test","cpu_quota":8000,"io_read_bandwidth":1048576000,"io_write_bandwidth":1048576000}`,
		},
	}

	for _, tc := range testCase {
		var r ResourceGroup
		err := json.Unmarshal([]byte(tc.src), &r)
		re.NoError(err)
		err = r.Validate()
		fmt.Println(tc.src)
		re.Equal(tc.success, err == nil)
		nr := r.IntoNodeConfig(1)
		o, err := json.Marshal(nr)
		re.NoError(err)
		re.Equal(tc.target, string(o))
	}
}
