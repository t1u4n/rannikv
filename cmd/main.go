package main

import (
	"flag"
	"github.com/TianLuan99/RanniKV.git/internal/rannift"
	"gopkg.in/yaml.v3"
	"os"
)

func main() {
	// Parse command line arguments
	cfgFile := flag.String("config", "config.yaml", "config file path")
	flag.Parse()

	// Read config file
	content, err := os.ReadFile(*cfgFile)
	if err != nil {
		panic(err)
	}

	// Unmarshal config file
	rnCfg := &rannift.RanniNodeConfig{}
	yaml.Unmarshal(content, rnCfg)

	// Initialize RanniNode
	rn, err := rannift.NewRanniNode(rnCfg)
	if err != nil {
		panic(err)
	}

	// Start RanniNode
	rn.Run()
}
