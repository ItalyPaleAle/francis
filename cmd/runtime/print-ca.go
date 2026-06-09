package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/italypaleale/francis/runtime"
)

// runPrintCA derives the cluster CA from the configured runtime PSKs and writes the PEM-encoded certificates to stdout
func runPrintCA(args []string) int {
	fs := flag.NewFlagSet("print-ca", flag.ExitOnError)
	var configPath string
	fs.StringVar(&configPath, "config", "config.yaml", "Path to the configuration file")
	_ = fs.Parse(args)

	cfg, err := loadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		return 1
	}

	psks, err := parsePSKs(cfg.RuntimePSKs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	bundle, err := runtime.CABundlePEM(psks...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error deriving CA: %v\n", err)
		return 1
	}

	for _, pem := range bundle {
		_, _ = os.Stdout.Write(pem)
	}
	return 0
}
