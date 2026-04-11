package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/appcompat"
)

func main() {
	var (
		addr             string
		user             string
		password         string
		iterations       int
		statementTimeout time.Duration
		reportPath       string
		jsonStdout       bool
	)
	flag.StringVar(&addr, "pg-addr", "", "ChronosDB pgwire address (host:port)")
	flag.StringVar(&user, "user", "chronos", "pgwire user")
	flag.StringVar(&password, "password", "chronos", "pgwire password")
	flag.IntVar(&iterations, "iterations", 10, "number of CRUD workload iterations")
	flag.DurationVar(&statementTimeout, "statement-timeout", 5*time.Second, "per-statement timeout")
	flag.StringVar(&reportPath, "report-json", "", "optional JSON report output path")
	flag.BoolVar(&jsonStdout, "json", false, "print the JSON report to stdout instead of the human summary")
	flag.Parse()

	report, err := appcompat.Run(context.Background(), appcompat.Config{
		Addr:             addr,
		User:             user,
		Password:         password,
		Iterations:       iterations,
		StatementTimeout: statementTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}

	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if reportPath != "" {
		if err := os.WriteFile(reportPath, append(payload, '\n'), 0o644); err != nil {
			log.Fatal(err)
		}
	}
	if jsonStdout {
		fmt.Printf("%s\n", payload)
	} else {
		fmt.Print(appcompat.SummaryString(report))
		if reportPath != "" {
			fmt.Printf("json report: %s\n", reportPath)
		}
	}
	if !report.Compatible {
		os.Exit(1)
	}
}
