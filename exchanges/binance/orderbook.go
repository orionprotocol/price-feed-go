package binance

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/adshao/go-binance"
	"github.com/nkryuchkov/tradingbot/logger"
	"github.com/nkryuchkov/tradingbot/storage"
	"github.com/pkg/errors"
)

const (
	priceURL    = "https://api.binance.com/api/v3/ticker/price"
	redisPrefix = "binance"
)

// Config represents an order book config
type Config struct {
	WsTimeout       string `json:"ws_timeout"`
	RequestInterval string `json:"request_interval"`
}

// OrderBook represents a Binance order book worker.
type orderBook struct {
	config                *Config
	log                   *logger.Logger
	database              *storage.Client
	requestInterval       time.Duration
	wsTimeout             time.Duration
	symbols               []string
	quitC                 chan os.Signal
	AggTradesC            chan *binance.WsAggTradeEvent
	TradesC               chan *binance.WsTradeEvent
	KlinesC               chan *binance.WsKlineEvent
	AllMarketMiniTickersC chan binance.WsAllMiniMarketsStatEvent
	AllMarketTickersC     chan binance.WsAllMarketsStatEvent
	PartialBookDepthsC    chan *binance.WsPartialDepthEvent
	DiffDepthsC           chan *binance.WsDepthEvent
	StopC                 chan struct{}
	stops                 []chan struct{}
	dones                 []chan struct{}
}

// New returns a new Binance order book worker.
func New(config *Config, log *logger.Logger, database *storage.Client, quitC chan os.Signal) (*orderBook, error) {
	wsTimeout, err := time.ParseDuration(config.WsTimeout)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse Binance WS timeout")
	}

	requestInterval, err := time.ParseDuration(config.RequestInterval)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse Binance request interval")
	}

	symbols, err := getSymbolList()
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse Binance symbol list")
	}

	log.Infof("Working with %v symbols on Binance", len(symbols))

	ob := &orderBook{
		config:                config,
		log:                   log,
		database:              database,
		wsTimeout:             wsTimeout,
		requestInterval:       requestInterval,
		symbols:               symbols,
		quitC:                 quitC,
		AggTradesC:            make(chan *binance.WsAggTradeEvent),
		TradesC:               make(chan *binance.WsTradeEvent),
		KlinesC:               make(chan *binance.WsKlineEvent),
		AllMarketMiniTickersC: make(chan binance.WsAllMiniMarketsStatEvent),
		AllMarketTickersC:     make(chan binance.WsAllMarketsStatEvent),
		PartialBookDepthsC:    make(chan *binance.WsPartialDepthEvent),
		DiffDepthsC:           make(chan *binance.WsDepthEvent, 10000),
		StopC:                 make(chan struct{}),
	}

	return ob, nil
}

// StartOrderBookWorker starts a new Binance order book worker.
func (b *orderBook) StartOrderBookWorker() chan struct{} {
	wsStopC := make(chan struct{})

	go func() {
		wsTicker := time.NewTicker(b.requestInterval)

		for {
			wsTimeoutTimer := time.NewTimer(b.wsTimeout)

			go func() {
				for {
					select {
					case <-b.StopC:
						return
					case depth := <-b.DiffDepthsC:
						data, err := json.Marshal(depth)
						if err != nil {
							b.log.Errorf("Could not marshal depth: %v", err)
						}
						b.database.Store(b.database.FormatKey("depth", depth.Symbol), float64(time.Now().Unix()), data)
					}
				}
			}()

		outer:
			for {
				select {
				case <-b.quitC:
					b.StopAll()
					wsStopC <- struct{}{}
					return
				case <-wsTicker.C:
					continue
				case <-wsTimeoutTimer.C:
					break outer
				}
			}

			b.StopAll()
		}
	}()

	for _, symbol := range b.symbols {
		go func(symbol string) {
			err := b.DiffDepths(symbol)
			if err != nil {
				b.log.Printf("Couldn't get diff depths on symbol %s: %v\n", symbol, err)
			}
		}(symbol)
	}

	return wsStopC
}

func (b *orderBook) AggTrades(symbol string) error {
	wsAggTradesHandler := func(event *binance.WsAggTradeEvent) {
		b.AggTradesC <- event
	}

	doneC, stopC, err := binance.WsAggTradeServe(symbol, wsAggTradesHandler, b.makeErrorHandler())
	if err != nil {
		return err
	}

	b.dones = append(b.dones, doneC)
	b.stops = append(b.stops, stopC)

	return nil
}

func (b *orderBook) Klines(symbol, interval string) error {
	wsKlineHandler := func(event *binance.WsKlineEvent) {
		b.KlinesC <- event
	}
	doneC, stopC, err := binance.WsKlineServe(symbol, interval, wsKlineHandler, b.makeErrorHandler())
	if err != nil {
		return err
	}

	b.dones = append(b.dones, doneC)
	b.stops = append(b.stops, stopC)

	return nil
}

func (b *orderBook) Trades(symbol string) error {
	wsTradesHandler := func(event *binance.WsTradeEvent) {
		b.TradesC <- event
	}
	doneC, stopC, err := binance.WsTradeServe(symbol, wsTradesHandler, b.makeErrorHandler())
	if err != nil {
		return err
	}

	b.dones = append(b.dones, doneC)
	b.stops = append(b.stops, stopC)

	return nil
}

func (b *orderBook) AllMarketMiniTickers() error {
	wsAllMarketMiniTickersHandler := func(event binance.WsAllMiniMarketsStatEvent) {
		b.AllMarketMiniTickersC <- event
	}
	doneC, stopC, err := binance.WsAllMiniMarketsStatServe(wsAllMarketMiniTickersHandler, b.makeErrorHandler())
	if err != nil {
		return err
	}

	b.dones = append(b.dones, doneC)
	b.stops = append(b.stops, stopC)

	return nil
}

func (b *orderBook) AllMarketTickers() error {
	wsAllMarketTickersHandler := func(event binance.WsAllMarketsStatEvent) {
		b.AllMarketTickersC <- event
	}
	doneC, stopC, err := binance.WsAllMarketsStatServe(wsAllMarketTickersHandler, b.makeErrorHandler())
	if err != nil {
		return err
	}

	b.dones = append(b.dones, doneC)
	b.stops = append(b.stops, stopC)

	return nil
}

func (b *orderBook) PartialBookDepths(symbol, levels string) error {
	wsPartialBookDepthsHandler := func(event *binance.WsPartialDepthEvent) {
		b.PartialBookDepthsC <- event
	}
	doneC, stopC, err := binance.WsPartialDepthServe(symbol, levels, wsPartialBookDepthsHandler, b.makeErrorHandler())
	if err != nil {
		return err
	}

	b.dones = append(b.dones, doneC)
	b.stops = append(b.stops, stopC)

	return nil
}

func (b *orderBook) DiffDepths(symbol string) error {
	wsDiffDepthsHandler := func(event *binance.WsDepthEvent) {
		b.DiffDepthsC <- event
	}
	doneC, stopC, err := binance.WsDepthServe(symbol, wsDiffDepthsHandler, b.makeErrorHandler())
	if err != nil {
		return err
	}

	b.dones = append(b.dones, doneC)
	b.stops = append(b.stops, stopC)

	return nil
}

func (b *orderBook) StopAll() {
	for _, c := range b.stops {
		c <- struct{}{}
	}

	for _, c := range b.dones {
		<-c
	}

	b.StopC <- struct{}{}
}

func (b *orderBook) makeErrorHandler() binance.ErrHandler {
	return func(err error) {
		b.log.Printf("Error in WS connection with Binance: %v", err)
	}
}

func getSymbolList() ([]string, error) {
	resp, err := http.Get(priceURL)
	if err != nil {
		return nil, err
	}

	var data []struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}

	if err = json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	symbols := make([]string, 0, len(data))

	for _, item := range data {
		symbols = append(symbols, item.Symbol)
	}

	return symbols, nil
}
