// Package config loads the gateway configuration from a YAML file.
package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the top-level gateway configuration.
type Config struct {
	BindAddr string   `yaml:"bind_addr"` // e.g. ":15003"
	Backends []string `yaml:"backends"`  // static list of "host:port" Spark Connect servers
}

// Load reads and validates a YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if c.BindAddr == "" {
		c.BindAddr = ":15003"
	}
	if len(c.Backends) == 0 {
		return nil, errors.New("config: at least one backend required in `backends`")
	}
	return &c, nil
}
