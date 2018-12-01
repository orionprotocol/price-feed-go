package storage

import (
	"math/big"
	"strconv"
	"strings"

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

// Store adds a new value and score in a sorted set with specified key.
func (c *Client) Store(key string, score float64, val interface{}) {
	c.client.ZAdd(key, redis.Z{
		Score:  score,
		Member: val,
	})
}

// FormatKey formats keys using given args separating them with a colon.
func (c *Client) FormatKey(args ...interface{}) string {
	s := make([]string, len(args))
	for i, v := range args {
		switch v.(type) {
		case string:
			s[i] = v.(string)
		case int64:
			s[i] = strconv.FormatInt(v.(int64), 10)
		case uint64:
			s[i] = strconv.FormatUint(v.(uint64), 10)
		case float64:
			s[i] = strconv.FormatFloat(v.(float64), 'f', 0, 64)
		case bool:
			if v.(bool) {
				s[i] = "1"
			} else {
				s[i] = "0"
			}
		case *big.Int:
			n := v.(*big.Int)
			if n != nil {
				s[i] = n.String()
			} else {
				s[i] = "0"
			}
		case *big.Rat:
			x := v.(*big.Rat)
			if x != nil {
				s[i] = x.FloatString(9)
			} else {
				s[i] = "0"
			}
		default:
			panic("Invalid type specified for conversion")
		}
	}
	return strings.Join(s, ":")
}
