package config

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	"github.com/nkryuchkov/tradingbot/api"
	"github.com/nkryuchkov/tradingbot/logger"

	"github.com/pkg/errors"
)

const (
	filename = "config.json"
)

// Config represents an application configuration.
type Config struct {
	Logger *logger.Config `json:"logger"`
	API    *api.Config    `json:"api"`
}

// FromFile reads a config from the file specified in `filename	.
func FromFile() (*Config, error) {
	configFilename := filename
	if len(os.Args) > 1 {
		configFilename = os.Args[1]
	}

	configFilePath, err := filepath.Abs(configFilename)
	if err != nil {
		return nil, errors.Wrapf(err, "could not find config absolute path")
	}

	log.Printf("Loading config: %v", configFilePath)

	configFile, err := os.Open(configFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "could not open config file")
	}

	defer func() {
		if err = configFile.Close(); err != nil {
			log.Printf("Could not close config file: %v", err)
		}
	}()

	var config Config
	if err := json.NewDecoder(configFile).Decode(&config); err != nil {
		return nil, errors.Wrapf(err, "could not read config file")
	}

	return &config, nil
}
