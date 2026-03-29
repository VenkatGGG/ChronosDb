package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/VenkatGGG/ChronosDb/internal/systemtest"
)

func main() {
	var cfg systemtest.ProcessNodeConfig
	flag.Uint64Var(&cfg.NodeID, "node-id", 0, "node identifier")
	flag.StringVar(&cfg.ClusterID, "cluster-id", "chronos-local", "cluster identifier")
	flag.Uint64Var(&cfg.StoreID, "store-id", 0, "store identifier (defaults to node-id)")
	flag.StringVar(&cfg.DataDir, "data-dir", "", "node data directory")
	flag.StringVar(&cfg.PGListenAddr, "pg-addr", "127.0.0.1:0", "pgwire listen address")
	flag.StringVar(&cfg.ObservabilityAddr, "obs-addr", "127.0.0.1:0", "observability listen address")
	flag.StringVar(&cfg.ControlAddr, "control-addr", "127.0.0.1:0", "control listen address")
	flag.Parse()

	node, err := systemtest.NewProcessNode(cfg)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := node.Run(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
