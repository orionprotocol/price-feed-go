package storage

import (
	"encoding/json"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/batonych/tradingbot/models"

	"github.com/batonych/tradingbot/logger"

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
	log    *logger.Logger
}

// New returns a new database client instance.
func New(cfg *Config, log *logger.Logger) *Client {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Endpoint,
		Password: cfg.Password,
		DB:       cfg.Database,
		PoolSize: cfg.PoolSize,
	})

	return &Client{
		client: client,
		log:    log,
	}
}

// Check sends a ping to the database.
func (c *Client) Check() (string, error) {
	return c.client.Ping().Result()
}

func (c *Client) LoadOrderBook(pair string) (map[string]*models.Depth, error) {
	result := make(map[string]*models.Depth)

	values, err := c.client.ZRangeWithScores(c.formatKey("depth", pair), 0, -1).Result()
	if err != nil {
		return nil, err
	}

	for _, v := range values {
		str, ok := v.Member.(string)
		if !ok {
			c.log.Errorf("%v is not a string, but %v", v.Member, v.Member)
			continue
		}

		var ob models.Depth
		if err = json.Unmarshal([]byte(str), &ob); err != nil {
			c.log.Errorf("Could not unmarshal %v: %v", str, err)
			continue
		}
		result[strconv.FormatInt(int64(v.Score), 10)] = &ob
	}

	return result, nil
}

func (c *Client) StoreOrderBook(pair string, depth *models.Depth) error {
	data, err := json.Marshal(depth)
	if err != nil {
		c.log.Errorf("Could not marshal depth: %v", err)
		return err
	}

	return c.store(c.formatKey("depth", pair), float64(time.Now().Unix()), string(data))
}

// store adds a new value and score in a sorted set with specified key.
func (c *Client) store(key string, score float64, val string) error {
	return c.client.ZAdd(key, redis.Z{
		Score:  score,
		Member: val,
	}).Err()
}

// formatKey formats keys using given args separating them with a colon.
func (c *Client) formatKey(args ...interface{}) string {
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
