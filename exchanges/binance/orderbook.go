package binance

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
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
	zero              = "0.00000000"
	orderBookMaxLimit = 1000
	apiInterval       = 1 * time.Second
)

// Config represents an order book config
type Config struct {
	WsTimeout       string `json:"ws_timeout"`
	RequestInterval string `json:"request_interval"`
}

// OrderBookAPI represents a Binance order book worker.
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
	cacheMu               sync.Mutex
	cache                 map[string]models.OrderBookInternal
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
		cache:                 make(map[string]models.OrderBookInternal),
	}

	if err = ob.fillSymbolListWithTestData(); err != nil {
		return nil, errors.Wrapf(err, "couldn't parse Binance symbol list")
	}

	return ob, nil
}

// StartOrderBookWorker starts a new Binance order book worker.
func (b *OrderBook) StartOrderBookWorker() {
	for _, symbol := range b.symbols {
		go func(symbol string) {
			err := b.SubscribeOrderBook(symbol)
			if err != nil {
				b.log.Printf("Couldn't get diff depths on symbol %s: %v", symbol, err)
			}
		}(symbol)
	}
}

func (b *OrderBook) GetOrderBook(symbol string) (models.OrderBookInternal, bool) {
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()

	ob, ok := b.cache[symbol]
	return ob, ok
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

// https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly
func (b *OrderBook) SubscribeOrderBook(symbol string) error {
	for ; ; <-time.Tick(b.requestInterval) {
		// Get a depth snapshot from https://www.binance.com/api/v1/depth?symbol=BNBBTC&limit=1000
		orderBook, err := b.getOrderBook(symbol, orderBookMaxLimit)

		b.log.Debugf("Got order book for symbol %v: %+v", symbol, orderBook)

		if err != nil {
			return errors.Wrapf(err, "could not get order book")
		}
		b.cacheMu.Lock()
		b.cache[symbol] = orderBook
		b.cacheMu.Unlock()

		// Buffer the events you receive from the stream
		wsDiffDepthsHandler := func(event *binance.WsDepthEvent) {
			if err = b.updateOrderBook(symbol, event); err != nil {
				b.log.Errorf("Could not update order book: %v", err)
			}
		}

		// Open a stream to wss://stream.binance.com:9443/ws/bnbbtc@depth
		doneC, _, err := binance.WsDepthServe(symbol, wsDiffDepthsHandler, b.makeErrorHandler())
		if err != nil {
			return err
		}

		<-doneC
	}
}

func (b *OrderBook) updateOrderBook(symbol string, event *binance.WsDepthEvent) error {
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()

	// Drop any event where u is <= lastUpdateId in the snapshot
	if event.UpdateID <= b.cache[symbol].LastUpdateID {
		return nil
	}

	for _, bid := range event.Bids {
		if bid.Quantity == zero {
			b.log.Debugf("deleting bid with price %v for symbol %v", bid.Price, symbol)
			delete(b.cache[symbol].Bids, bid.Price)
			continue
		}

		b.cache[symbol].Bids[bid.Price] = bid.Quantity
	}

	for _, ask := range event.Asks {
		if ask.Quantity == zero {
			b.log.Debugf("deleting ask with price %v for symbol %v", ask.Price, symbol)
			delete(b.cache[symbol].Asks, ask.Price)
			continue
		}

		b.cache[symbol].Asks[ask.Price] = ask.Quantity
	}

	if err := b.database.StoreOrderBookInternal(symbol, b.cache[symbol]); err != nil {
		b.log.Errorf("Could not store to database: %v", err)
	}

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

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fillSymbolList received bad status code: %v", resp.StatusCode)
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

func (b *OrderBook) fillSymbolListWithTestData() error {
	b.symbols = binanceSymbols
	return nil
}

func (b *OrderBook) getOrderBook(symbol string, depth int) (response models.OrderBookInternal, err error) {
	orderBookURL, err := b.makeOrderBookURL(symbol, depth)
	if err != nil {
		return models.OrderBookInternal{}, errors.Wrapf(err, "could not make order book URL")
	}

	resp, err := http.Get(orderBookURL)
	if err != nil {
		return models.OrderBookInternal{}, err
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		time.Sleep(apiInterval)
	} else if resp.StatusCode != http.StatusOK {
		return models.OrderBookInternal{}, fmt.Errorf("getOrderBook received bad status code: %v", resp.StatusCode)
	}

	var data models.OrderBookResponse

	if err = json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return models.OrderBookInternal{}, err
	}

	return models.SerializeBinanceOrderBookREST(data), nil
}

func (b *OrderBook) makeOrderBookURL(symbol string, depth int) (string, error) {
	u, err := url.Parse(depthURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("symbol", symbol)
	q.Set("limit", strconv.Itoa(depth))
	u.RawQuery = q.Encode()
	return u.String(), nil
}
