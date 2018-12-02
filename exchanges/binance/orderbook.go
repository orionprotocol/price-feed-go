package binance

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/adshao/go-binance"
	"github.com/batonych/tradingbot/logger"
	"github.com/batonych/tradingbot/models"
	"github.com/batonych/tradingbot/storage"
	"github.com/pkg/errors"
)

const (
	priceURL          = "https://api.binance.com/api/v3/ticker/price"
	depthURL          = "https://api.binance.com/api/v1/depth"
	redisPrefix       = "binance"
	orderBookMaxLimit = 1000
)

// Config represents an order book config
type Config struct {
	WsTimeout       string `json:"ws_timeout"`
	RequestInterval string `json:"request_interval"`
}

// OrderBook represents a Binance order book worker.
type OrderBook struct {
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
func New(config *Config, log *logger.Logger, database *storage.Client, quitC chan os.Signal) (*OrderBook, error) {
	wsTimeout, err := time.ParseDuration(config.WsTimeout)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse Binance WS timeout")
	}

	requestInterval, err := time.ParseDuration(config.RequestInterval)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse Binance request interval")
	}

	ob := &OrderBook{
		config:                config,
		log:                   log,
		database:              database,
		wsTimeout:             wsTimeout,
		requestInterval:       requestInterval,
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

	if err = ob.fillSymbolList(); err != nil {
		return nil, errors.Wrapf(err, "couldn't parse Binance symbol list")
	}

	return ob, nil
}

// StartOrderBookWorker starts a new Binance order book worker.
func (b *OrderBook) StartOrderBookWorker() chan struct{} {
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
						data := models.SerializeBinanceOrderBook(depth)
						if err := b.database.StoreOrderBook(depth.Symbol, data); err != nil {
							b.log.Errorf("Could not store to database: %v")
						}
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
				b.log.Printf("Couldn't get diff depths on symbol %s: %v", symbol, err)
			}
		}(symbol)
	}

	return wsStopC
}

func (b *OrderBook) AggTrades(symbol string) error {
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

func (b *OrderBook) Klines(symbol, interval string) error {
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

func (b *OrderBook) Trades(symbol string) error {
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

func (b *OrderBook) AllMarketMiniTickers() error {
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

func (b *OrderBook) AllMarketTickers() error {
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

func (b *OrderBook) PartialBookDepths(symbol, levels string) error {
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

func (b *OrderBook) DiffDepths(symbol string) error {
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

func (b *OrderBook) StopAll() {
	for _, c := range b.stops {
		c <- struct{}{}
	}

	for _, c := range b.dones {
		<-c
	}

	b.StopC <- struct{}{}
}

func (b *OrderBook) makeErrorHandler() binance.ErrHandler {
	return func(err error) {
		b.log.Printf("Error in WS connection with Binance: %v", err)
	}
}

func (b *OrderBook) fillSymbolList() error {
	resp, err := http.Get(priceURL)
	if err != nil {
		return err
	}

	var data []struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}

	if err = json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}

	symbols := make([]string, 0, len(data))

	for _, item := range data {
		symbols = append(symbols, item.Symbol)
	}

	b.log.Infof("Working with %v symbols on Binance", len(symbols))

	b.symbols = symbols
	return nil
}

func (b *OrderBook) buildDepthURL(symbol string) (string, error) {
	u, err := url.Parse(depthURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("symbol", symbol)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
