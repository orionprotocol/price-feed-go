package redis

import (
	"gopkg.in/redis.v3"
)

type Config struct {
	Endpoint string `json:"endpoint"`
	Password string `json:"password"`
	Database int64  `json:"database"`
	PoolSize int    `json:"poolSize"`
}

type Client struct {
	client *redis.Client
}

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
