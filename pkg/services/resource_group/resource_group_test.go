package resourcegroup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddr(t *testing.T) {
	re := require.New(t)
	nodes := []struct {
		Address    string
		StatusAddr string
	}{
		{Address: "127.0.0.1:4000", StatusAddr: "192.168.130.22:10080"},
		{Address: "0.0.0.0:4000", StatusAddr: "192.168.130.22:10080"},
		{Address: "localhost:4000", StatusAddr: "192.168.130.22:10080"},
		{Address: "192.168.130.22:4000", StatusAddr: "0.0.0.0:10080"},
		{Address: "192.168.130.22:4000", StatusAddr: "127.0.0.1:10080"},
		{Address: "192.168.130.22:4000", StatusAddr: "localhost:10080"},
	}
	for i := range nodes {
		addr, statusAddr := ResolveLoopBackAddr(nodes[i].Address, nodes[i].StatusAddr)
		re.Equal(statusAddr, "192.168.130.22:10080")
		re.Equal(addr, "192.168.130.22:4000")
	}
}
