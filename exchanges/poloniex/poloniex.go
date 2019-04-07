package poloniex

import (
	"os"
	"time"

	"github.com/jyap808/go-poloniex"

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
	poloniex        *poloniex.Poloniex
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
		symbols:         models.PoloniexSymbols,
		poloniex:        poloniex.New("", ""),
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
		for _, v := range models.PoloniexCandlestickIntervalList {
			go func(s int) {
				w.initCandlesticks(symbol, s)
			}(v)
		}
	}
	w.log.Infof("Poloniex cache reloaded")
}

func (w *Worker) SubscribeCandlestickAll(symbol string) {
	for _, v := range models.PoloniexCandlestickIntervalList {
		go func(s int) {
			w.initCandlesticks(symbol, s)

			if err := w.SubscribeCandlestick(symbol, s); err != nil {
				w.log.Errorf("Could not subscribe to candlestick interval %v symbol %v: %v", v, symbol, err)
			}
		}(v)
	}
}

func (w *Worker) initCandlesticks(symbol string, interval int) {
	candlesticks, err := w.poloniex.ChartData(symbol, interval, time.Now().AddDate(0, 0, -15), time.Now())
	if err != nil {
		w.log.Errorf("Could not load candlesticks from Poloniex REST API with interval %v and symbol %v: %v",
			interval, symbol, err)

		return
	}

	for _, k := range candlesticks {
		if err := w.updateCandlestickAPI(symbol, interval, k); err != nil {
			w.log.Errorf("Could not update candlesticks from REST API: %v", err)
		}
	}
}

func (w *Worker) updateCandlestickAPI(symbol string, interval int, candlestick *poloniex.CandleStick) error {
	if err := w.database.StoreCandlestickPoloniexAPI(symbol, models.PoloniexIntervalToBinance(interval), candlestick); err != nil {
		w.log.Errorf("Could not store candlestick from REST API to database: %v", err)
	}

	return nil
}

func (w *Worker) SubscribeCandlestick(symbol string, interval int) error {
	for ; ; <-time.Tick(w.requestInterval) {
		candles, err := w.poloniex.ChartData(symbol, interval, time.Now().Add(-3*w.requestInterval), time.Now().Add(3*w.requestInterval))

		if err != nil {
			w.log.Errorf("Could not get latest tick on poloniex: %v", err)
		}

		for _, candle := range candles {
			if err := w.updateCandlestickAPI(symbol, interval, candle); err != nil {
				w.log.Errorf("Could not update candlesticks from REST API: %v", err)
			}
		}
	}
}
