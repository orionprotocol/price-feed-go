package storage

import (
	"gopkg.in/redis.v3"
)

// Config represents a database configuration.
type Config struct {
	Endpoint string `json:"endpoint"`
	Password string `json:"password"`
	Database int64  `json:"database"`
	PoolSize int    `json:"poolSize"`
}

// Client represents a database client instance.
type Client struct {
	client *redis.Client
}

// New returns a new database client instance.
func New(cfg *Config) *Client {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Endpoint,
		Password: cfg.Password,
		DB:       cfg.Database,
		PoolSize: cfg.PoolSize,
	})
	return &Client{
		client: client,
	}
}

// Check sends a ping to the database.
func (c *Client) Check() (string, error) {
	return c.client.Ping().Result()
}
