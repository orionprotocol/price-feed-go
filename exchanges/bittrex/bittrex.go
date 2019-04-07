package bittrex

import (
	"os"
	"time"

	"github.com/toorop/go-bittrex"

	"price-feed/logger"
	"price-feed/models"
	"price-feed/storage"
)

type Config struct {
	RequestInterval string `json:"request_interval"`
}

type Worker struct {
	config          *Config
	log             *logger.Logger
	database        *storage.Client
	requestInterval time.Duration
	symbols         []string
	bittrex         *bittrex.Bittrex
	quit            chan os.Signal
}

func NewWorker(config *Config, log *logger.Logger, database *storage.Client, quit chan os.Signal) (*Worker, error) {
	interval, err := time.ParseDuration(config.RequestInterval)
	if err != nil {
		return nil, err
	}

	w := &Worker{
		config:          config,
		log:             log,
		database:        database,
		requestInterval: interval,
		symbols:         models.BittrexSymbols,
		bittrex:         bittrex.New("", ""),
		quit:            quit,
	}

	return w, nil
}

func (w *Worker) Start() {
	for _, symbol := range w.symbols {
		// go func(symbol string) {
		// 	err := w.SubscribeOrderBook(symbol)
		// 	if err != nil {
		// 		w.log.Printf("Couldn't get diff depths on symbol %s: %v", symbol, err)
		// 	}
		// }(symbol)
		go w.SubscribeCandlestickAll(symbol)
	}
}

func (w *Worker) Reload() {
	for _, symbol := range w.symbols {
		for _, v := range models.BittrexCandlestickIntervalList {
			go func(s string) {
				w.initCandlesticks(symbol, s)
			}(v)
		}
	}
	w.log.Infof("Bittrex cache reloaded")
}

func (w *Worker) SubscribeCandlestickAll(symbol string) {
	for _, v := range models.BittrexCandlestickIntervalList {
		go func(s string) {
			w.initCandlesticks(symbol, s)

			if err := w.SubscribeCandlestick(symbol, s); err != nil {
				w.log.Errorf("Could not subscribe to candlestick interval %v symbol %v: %v", v, symbol, err)
			}
		}(v)
	}
}

func (w *Worker) initCandlesticks(symbol, interval string) {
	candlesticks, err := w.bittrex.GetTicks(symbol, interval)
	if err != nil {
		w.log.Errorf("Could not load candlesticks from Bittrex REST API with interval %v and symbol %v: %v",
			interval, symbol, err)

		return
	}

	for _, k := range candlesticks {
		if err := w.updateCandlestickAPI(symbol, interval, &k); err != nil {
			w.log.Errorf("Could not update candlesticks from REST API: %v", err)
		}
	}
}

func (w *Worker) updateCandlestickAPI(symbol, interval string, candlestick *bittrex.Candle) error {
	if err := w.database.StoreCandlestickBittrexAPI(symbol, models.BittrexIntervalToBinance(interval), candlestick); err != nil {
		w.log.Errorf("Could not store candlestick from REST API to database: %v", err)
	}

	return nil
}

func (w *Worker) SubscribeCandlestick(symbol, interval string) error {
	for ; ; <-time.Tick(w.requestInterval) {
		candles, err := w.bittrex.GetLatestTick(symbol, interval)
		if err != nil {
			w.log.Errorf("Could not get latest tick on bittrex: %v", err)
		}

		for _, candle := range candles {
			if err := w.updateCandlestickAPI(symbol, interval, &candle); err != nil {
				w.log.Errorf("Could not update candlesticks from REST API: %v", err)
			}
		}
	}
}
