package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"path/filepath"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/systemtest"
)

func main() {
	var (
		nodeBinary   string
		artifactRoot string
		handoffPath  string
	)
	flag.StringVar(&nodeBinary, "node-binary", "", "path to chronos-node binary")
	flag.StringVar(&artifactRoot, "artifact-root", "", "artifact output directory")
	flag.StringVar(&handoffPath, "handoff", "", "optional path to handoff.json for a single-scenario run")
	flag.Parse()

	if nodeBinary == "" {
		log.Fatal("node-binary is required")
	}
	if artifactRoot == "" {
		log.Fatal("artifact-root is required")
	}

	ctx := context.Background()
	if handoffPath == "" {
		_, err := systemtest.ExecuteFaultMatrix(ctx, systemtest.FaultMatrixConfig{
			ArtifactRoot:      artifactRoot,
			ControllerFactory: systemtest.NewExternalControllerFactory(nodeBinary, filepath.Join(artifactRoot, "cluster"), nil, nil),
		})
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	bundle, err := systemtest.LoadHandoffBundle(handoffPath)
	if err != nil {
		log.Fatal(err)
	}
	scenario, err := systemtest.ScenarioFromManifest(bundle.Manifest)
	if err != nil {
		log.Fatal(err)
	}
	controller, err := systemtest.NewExternalProcessController(systemtest.ExternalProcessControllerConfig{
		Nodes:        scenario.Nodes,
		BinaryPath:   nodeBinary,
		BaseDir:      filepath.Join(artifactRoot, "cluster"),
		StartTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if closeErr := controller.Close(); closeErr != nil && !errors.Is(closeErr, context.Canceled) {
			log.Printf("close controller: %v", closeErr)
		}
	}()
	report, runErr := (systemtest.Runner{Controller: controller}).Run(ctx, scenario)
	artifacts, err := systemtest.BuildRunArtifacts(scenario, report, controller.SnapshotNodeLogs())
	if err != nil {
		log.Fatal(err)
	}
	if assertErr := systemtest.ValidateArtifactAssertions(artifacts, systemtest.DefaultCorrectnessAssertions()...); assertErr != nil {
		artifacts.Summary.Status = "fail"
		artifacts.Summary.Failure = assertErr.Error()
		runErr = errors.Join(runErr, assertErr)
	}
	if err := artifacts.WriteDir(artifactRoot); err != nil {
		log.Fatal(err)
	}
	if err := systemtest.WriteHandoffBundle(artifactRoot, scenario); err != nil {
		log.Fatal(err)
	}
	if runErr != nil {
		log.Fatal(runErr)
	}
}
