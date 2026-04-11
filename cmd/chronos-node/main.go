package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/VenkatGGG/ChronosDb/internal/node"
)

func main() {
	var cfg node.Config
	flag.Uint64Var(&cfg.NodeID, "node-id", 0, "node identifier")
	flag.StringVar(&cfg.ClusterID, "cluster-id", "chronos-local", "cluster identifier")
	flag.Uint64Var(&cfg.StoreID, "store-id", 0, "store identifier (defaults to node-id)")
	flag.StringVar(&cfg.BootstrapPath, "bootstrap-path", "", "path to persistent cluster bootstrap manifest")
	flag.StringVar(&cfg.DataDir, "data-dir", "", "node data directory")
	flag.StringVar(&cfg.PGListenAddr, "pg-addr", "127.0.0.1:0", "pgwire listen address")
	flag.StringVar(&cfg.PGWireUser, "pg-user", node.DefaultPGWireUser, "pgwire login user")
	flag.StringVar(&cfg.PGWirePassword, "pg-password", node.DefaultPGWirePassword, "pgwire login password")
	flag.StringVar(&cfg.ObservabilityAddr, "obs-addr", "127.0.0.1:0", "observability listen address")
	flag.StringVar(&cfg.ControlAddr, "control-addr", "127.0.0.1:0", "control listen address")
	flag.StringVar(&cfg.AdminBearerToken, "admin-bearer-token", "", "optional bearer token required for control and observability endpoints")
	flag.Parse()

	nodeProcess, err := node.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := nodeProcess.Run(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
