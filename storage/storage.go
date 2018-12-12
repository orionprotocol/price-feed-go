package storage

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/adshao/go-binance"

	"github.com/batonych/tradingbot/logger"
	"github.com/batonych/tradingbot/models"

	"gopkg.in/redis.v3"
)

const (
	roundTime             = 10 * time.Millisecond
	orderBookExpiration   = 1 * time.Minute
	candlestickExpiration = 5 * 12 * 30 * 24 * time.Hour
	day                   = 24 * time.Hour
	threeDays             = 3 * day
	week                  = 7 * day
	millisecond           = 1 * time.Millisecond
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

func (c *Client) LoadOrderBook(pair string) (models.OrderBookAPI, error) {
	result, err := c.client.ZRangeWithScores(c.formatKey("depth", pair), -2, -1).Result()
	if err != nil {
		return models.OrderBookAPI{}, err
	}

	if len(result) == 0 {
		return models.EmptyOrderBook, err
	}

	str, ok := result[0].Member.(string)
	if !ok {
		return models.OrderBookAPI{}, fmt.Errorf("%v is not string, but %v", result[0].Member, result[0].Member)
	}

	var ob models.OrderBookAPI
	if err = json.Unmarshal([]byte(str), &ob); err != nil {
		return models.OrderBookAPI{}, fmt.Errorf("could not unmarshal %v: %v", str, err)
	}

	return ob, nil
}

func (c *Client) StoreOrderBook(pair string, depth *models.OrderBookAPI) error {
	data, err := json.Marshal(depth)
	if err != nil {
		c.log.Errorf("Could not marshal depth: %v", err)
		return err
	}

	return c.store(c.formatKey("depth", pair), float64(time.Now().Unix()), string(data))
}

func (c *Client) LoadOrderBookInternal(symbol string, depth int) (models.OrderBookAPI, error) {
	result, err := c.client.ZRangeWithScores(c.formatKey("orderBook", symbol), -1, -1).Result()
	if err != nil {
		return models.OrderBookAPI{}, err
	}

	if len(result) == 0 {
		return models.EmptyOrderBook, err
	}

	str, ok := result[0].Member.(string)
	if !ok {
		return models.OrderBookAPI{}, fmt.Errorf("%v is not string, but %v", result[0].Member, result[0].Member)
	}

	var ob models.OrderBookInternal
	if err = json.Unmarshal([]byte(str), &ob); err != nil {
		return models.OrderBookAPI{}, fmt.Errorf("could not unmarshal %v: %v", str, err)
	}

	orderBook := ob.Format(depth)

	c.log.Debugf("LoadOrderBookInternal result: %+v", orderBook)
	return orderBook, nil
}

func (c *Client) LoadCandlestickList(symbol, interval string, timeStart, timeEnd int64) ([]models.Candle, error) {
	var timeStartRounded, timeEndRounded time.Time
	switch interval {
	case "1d":
		timeStartRounded = time.Unix(timeStart, 0).Truncate(day)
	case "3d":
		timeStartRounded = time.Unix(timeStart, 0).Truncate(threeDays)
	case "1w":
		timeStartRounded = time.Unix(timeStart, 0).Truncate(week)
	case "1M":
		timeStartDefault := time.Unix(timeStart, 0)
		timeStartRounded = time.Date(timeStartDefault.Year(), timeStartDefault.Month(),
			1, 0, 0, 0, int(millisecond), nil)
	default:
		intervalDuration, err := time.ParseDuration(interval)
		if err != nil {
			return nil, fmt.Errorf("could not parse interval: %v", err)
		}

		timeStartRounded = time.Unix(timeStart, 0).Truncate(intervalDuration)
	}

	timeEndRounded = time.Unix(timeEnd, 0)

	result, err := c.client.ZRangeByScoreWithScores(c.formatKey("candlestick", symbol, interval),
		redis.ZRangeByScore{
			Min: strconv.FormatInt(timeStartRounded.Unix(), 10),
			Max: strconv.FormatInt(timeEndRounded.Unix(), 10),
		}).Result()
	if err != nil {
		return nil, err
	}

	candleList := make([]models.Candle, 0, len(result))

	for _, v := range result {
		str, ok := v.Member.(string)
		if !ok {
			return nil, fmt.Errorf("%v is not string, but %v", v.Member, v.Member)
		}

		var ob models.Candle
		if err = json.Unmarshal([]byte(str), &ob); err != nil {
			return nil, fmt.Errorf("could not unmarshal %v: %v", str, err)
		}

		candleList = append(candleList, ob)
	}

	c.log.Debugf("LoadCandlestickList result: %+v", candleList)
	return candleList, nil
}

func (c *Client) StoreOrderBookInternal(symbol string, orderBook models.OrderBookInternal) error {
	data, err := json.Marshal(orderBook)
	if err != nil {
		c.log.Errorf("Could not marshal order book: %v", err)
		return err
	}

	if err = c.purge(c.formatKey("orderBook", symbol), 0, int64(time.Now().Add(-orderBookExpiration).Unix())); err != nil {
		return err
	}

	return c.store(c.formatKey("orderBook", symbol), float64(time.Now(). /*.Round(roundTime)*/ Unix()), string(data))
}

func (c *Client) StoreCandlestick(symbol, interval string, candlestick *binance.WsKlineEvent) error {
	candle := models.CandleFromEvent(candlestick)

	data, err := json.Marshal(candle)
	if err != nil {
		c.log.Errorf("Could not marshal candlestick: %v", err)
		return err
	}

	return c.storeCandlestick(symbol, interval, float64(candle.TimeStart/1000), data)
}

func (c *Client) StoreCandlestickAPI(symbol, interval string, candlestick *binance.Kline) error {
	data, err := json.Marshal(models.CandleFromAPI(candlestick))
	if err != nil {
		c.log.Errorf("Could not marshal candlestick: %v", err)
		return err
	}

	return c.storeCandlestick(symbol, interval, float64(candlestick.OpenTime/1000), data)
}

func (c *Client) storeCandlestick(symbol, interval string, openTime float64, candlestick []byte) error {
	/*if err := c.purge(c.formatKey("candlestick", symbol, interval), 0, int64(time.Now().UTC().Add(-candlestickExpiration).Unix())); err != nil {
		return err
	}*/

	return c.store(c.formatKey("candlestick", symbol, interval), openTime, string(candlestick))
}

// store adds a new value and score in a sorted set with specified key.
func (c *Client) store(key string, score float64, val string) error {
	return c.client.ZAdd(key, redis.Z{
		Score:  score,
		Member: val,
	}).Err()
}

func (c *Client) purge(key string, min, max int64) error {
	return c.client.ZRemRangeByScore(key, strconv.FormatInt(min, 10), strconv.FormatInt(max, 10)).Err()
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
