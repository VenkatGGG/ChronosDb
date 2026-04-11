package demo

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
	"github.com/VenkatGGG/ChronosDb/internal/node"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

const (
	DefaultClusterID      = "chronos-demo"
	DefaultConsoleAddress = "127.0.0.1:8080"
)

var defaultNodePorts = []struct {
	NodeID      uint64
	StoreID     uint64
	PGAddr      string
	ObsAddr     string
	ControlAddr string
}{
	{NodeID: 1, StoreID: 11, PGAddr: "127.0.0.1:26257", ObsAddr: "127.0.0.1:18081", ControlAddr: "127.0.0.1:19081"},
	{NodeID: 2, StoreID: 12, PGAddr: "127.0.0.1:26258", ObsAddr: "127.0.0.1:18082", ControlAddr: "127.0.0.1:19082"},
	{NodeID: 3, StoreID: 13, PGAddr: "127.0.0.1:26259", ObsAddr: "127.0.0.1:18083", ControlAddr: "127.0.0.1:19083"},
}

// BootstrapPath returns the canonical bootstrap manifest path under one demo root.
func BootstrapPath(rootDir string) string {
	return filepath.Join(rootDir, "bootstrap.json")
}

// DefaultNodeConfigs returns deterministic process-node configs for the seeded demo cluster.
func DefaultNodeConfigs(rootDir, clusterID string) []node.Config {
	bootstrapPath := BootstrapPath(rootDir)
	configs := make([]node.Config, 0, len(defaultNodePorts))
	for _, spec := range defaultNodePorts {
		configs = append(configs, node.Config{
			NodeID:            spec.NodeID,
			ClusterID:         clusterID,
			StoreID:           spec.StoreID,
			BootstrapPath:     bootstrapPath,
			DataDir:           filepath.Join(rootDir, fmt.Sprintf("node-%d", spec.NodeID)),
			PGListenAddr:      spec.PGAddr,
			ObservabilityAddr: spec.ObsAddr,
			ControlAddr:       spec.ControlAddr,
		})
	}
	return configs
}

// DefaultObservabilityURLs returns the admin snapshot base URLs for the seeded demo nodes.
func DefaultObservabilityURLs() []string {
	out := make([]string, 0, len(defaultNodePorts))
	for _, node := range defaultNodePorts {
		out = append(out, "http://"+node.ObsAddr)
	}
	return out
}

// DefaultBootstrapManifest returns the deterministic 3-node demo placement layout.
func DefaultBootstrapManifest(clusterID string) (chronosruntime.BootstrapManifest, error) {
	nodes := make([]chronosruntime.BootstrapNode, 0, len(defaultNodePorts))
	for _, node := range defaultNodePorts {
		nodes = append(nodes, chronosruntime.BootstrapNode{
			NodeID:  node.NodeID,
			StoreID: node.StoreID,
		})
	}

	userSplit := storage.GlobalTablePrimaryKey(7, encodedIntPrimaryKey(50))
	orderSplit := storage.GlobalTablePrimaryKey(9, encodedIntPrimaryKey(100))
	return chronosruntime.BuildBootstrapManifest(clusterID, nodes, []meta.RangeDescriptor{
		{
			RangeID:              101,
			Generation:           1,
			StartKey:             storage.GlobalTablePrimaryPrefix(7),
			EndKey:               userSplit,
			Replicas:             voters(101, 1, 102, 2, 103, 3),
			LeaseholderReplicaID: 101,
		},
		{
			RangeID:              102,
			Generation:           1,
			StartKey:             userSplit,
			EndKey:               storage.GlobalTablePrimaryPrefix(8),
			Replicas:             voters(104, 1, 105, 2, 106, 3),
			LeaseholderReplicaID: 106,
		},
		{
			RangeID:              103,
			Generation:           1,
			StartKey:             storage.GlobalTablePrimaryPrefix(9),
			EndKey:               orderSplit,
			Replicas:             voters(107, 1, 108, 2, 109, 3),
			LeaseholderReplicaID: 108,
		},
		{
			RangeID:              104,
			Generation:           1,
			StartKey:             orderSplit,
			EndKey:               storage.GlobalTablePrimaryPrefix(10),
			Replicas:             voters(110, 1, 111, 2, 112, 3),
			LeaseholderReplicaID: 110,
		},
	})
}

// DefaultSmokeQueries returns a repeatable end-to-end query set for the seeded demo cluster.
func DefaultSmokeQueries() []string {
	return []string{
		"insert into users (id, name, email) values (7, 'alice', 'a@example.com')",
		"insert into users (id, name, email) values (70, 'bob', 'b@example.com')",
		"insert into orders (id, user_id, region, sales) values (1, 7, 'us-east1', 120)",
		"insert into orders (id, user_id, region, sales) values (200, 70, 'us-west1', 180)",
		"select id, name from users where id >= 7 and id < 80",
		"select region, sum(sales) from orders group by region",
		"select u.name, o.sales from users u join orders o on u.id = o.user_id",
	}
}

func voters(replicaID1, nodeID1, replicaID2, nodeID2, replicaID3, nodeID3 uint64) []meta.ReplicaDescriptor {
	return []meta.ReplicaDescriptor{
		{ReplicaID: replicaID1, NodeID: nodeID1, Role: meta.ReplicaRoleVoter},
		{ReplicaID: replicaID2, NodeID: nodeID2, Role: meta.ReplicaRoleVoter},
		{ReplicaID: replicaID3, NodeID: nodeID3, Role: meta.ReplicaRoleVoter},
	}
}

func encodedIntPrimaryKey(value int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value)^(uint64(1)<<63))
	return buf[:]
}
