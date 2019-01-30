package binance

import (
	"context"
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
	candlestickLimit  = 1000
	apiInterval       = 1 * time.Second
)

// Config represents an order book config
type Config struct {
	WsTimeout       string `json:"ws_timeout"`
	RequestInterval string `json:"request_interval"`
}

// OrderBookAPI represents a Binance order book worker.
type Worker struct {
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
	orderBookCacheMu      sync.Mutex
	orderBookCache        map[string]models.OrderBookInternal
}

type SymbolInterval struct {
	Symbol   string
	Interval string
}

// NewWorker returns a new Binance worker.
func NewWorker(config *Config, log *logger.Logger, database *storage.Client, quitC chan os.Signal) (*Worker, error) {
	wsTimeout, err := time.ParseDuration(config.WsTimeout)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse Binance WS timeout")
	}

	requestInterval, err := time.ParseDuration(config.RequestInterval)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse Binance request interval")
	}

	ob := &Worker{
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
		orderBookCache:        make(map[string]models.OrderBookInternal),
	}

	if err = ob.fillSymbolListWithTestData(); err != nil {
		return nil, errors.Wrapf(err, "couldn't parse Binance symbol list")
	}

	return ob, nil
}

// Start starts a new Binance worker.
func (b *Worker) Start() {
	for _, symbol := range b.symbols {
		go func(symbol string) {
			err := b.SubscribeOrderBook(symbol)
			if err != nil {
				b.log.Printf("Couldn't get diff depths on symbol %s: %v", symbol, err)
			}
		}(symbol)
		go b.SubscribeCandlestickAll(symbol)
	}
}

func (b *Worker) GetOrderBook(symbol string) (models.OrderBookInternal, bool) {
	b.orderBookCacheMu.Lock()
	defer b.orderBookCacheMu.Unlock()

	ob, ok := b.orderBookCache[symbol]
	return ob, ok
}

func (b *Worker) AggTrades(symbol string) error {
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

func (b *Worker) Klines(symbol, interval string) error {
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

func (b *Worker) Trades(symbol string) error {
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

func (b *Worker) AllMarketMiniTickers() error {
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

func (b *Worker) AllMarketTickers() error {
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

func (b *Worker) PartialBookDepths(symbol, levels string) error {
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

func (b *Worker) DiffDepths(symbol string) error {
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
func (b *Worker) SubscribeOrderBook(symbol string) error {
	for ; ; <-time.Tick(b.requestInterval) {
		// Get a depth snapshot from https://www.binance.com/api/v1/depth?symbol=BNBBTC&limit=1000
		orderBook, err := b.getOrderBook(symbol, orderBookMaxLimit)

		// b.log.Debugf("Got order book for symbol %v: %+v", symbol, orderBook)

		if err != nil {
			return errors.Wrapf(err, "could not get order book")
		}
		b.orderBookCacheMu.Lock()
		b.orderBookCache[symbol] = orderBook
		b.orderBookCacheMu.Unlock()

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

func (b *Worker) SubscribeCandlestickAll(symbol string) {
	for _, v := range models.BinanceCandlestickIntervalList {
		go func(s string) {
			b.initCandlesticks(symbol, s)

			if err := b.SubscribeCandlestick(symbol, s); err != nil {
				b.log.Errorf("Could not subscribe to candlestick interval %v symbol %v: %v", v, symbol, err)
			}
		}(v)
	}
}

func (b *Worker) initCandlesticks(symbol, interval string) {
	client := binance.NewClient("", "")
	candlesticks, err := client.NewKlinesService().Symbol(symbol).
		Interval(interval).Limit(candlestickLimit).Do(context.Background())
	if err != nil {
		b.log.Errorf("Could not load candlesticks from REST API with interval %v and symbol %v: %v",
			interval, symbol, err)

		return
	}

	for _, k := range candlesticks {
		if err := b.updateCandlestickAPI(symbol, interval, k); err != nil {
			b.log.Errorf("Could not update candlesticks from REST API: %v", err)
		}
	}
}

func (b *Worker) SubscribeCandlestick(symbol, interval string) error {
	for ; ; <-time.Tick(b.requestInterval) {
		wsCandlestickHandler := func(event *binance.WsKlineEvent) {
			if err := b.updateCandlestick(symbol, interval, event); err != nil {
				b.log.Errorf("Could not update order book: %v", err)
			}
		}

		// Open a stream to wss://stream.binance.com:9443/ws/bnbbtc@depth
		doneC, _, err := binance.WsKlineServe(symbol, interval, wsCandlestickHandler, b.makeErrorHandler())
		if err != nil {
			return err
		}

		<-doneC
	}
}

func (b *Worker) updateOrderBook(symbol string, event *binance.WsDepthEvent) error {
	b.orderBookCacheMu.Lock()
	defer b.orderBookCacheMu.Unlock()

	// Drop any event where u is <= lastUpdateId in the snapshot
	if event.UpdateID <= b.orderBookCache[symbol].LastUpdateID {
		return nil
	}

	for _, bid := range event.Bids {
		if bid.Quantity == zero {
			// b.log.Debugf("deleting bid with price %v for symbol %v", bid.Price, symbol)
			delete(b.orderBookCache[symbol].Bids, bid.Price)
			continue
		}

		b.orderBookCache[symbol].Bids[bid.Price] = bid.Quantity
	}

	for _, ask := range event.Asks {
		if ask.Quantity == zero {
			// b.log.Debugf("deleting ask with price %v for symbol %v", ask.Price, symbol)
			delete(b.orderBookCache[symbol].Asks, ask.Price)
			continue
		}

		b.orderBookCache[symbol].Asks[ask.Price] = ask.Quantity
	}

	if err := b.database.StoreOrderBookInternal(symbol, b.orderBookCache[symbol]); err != nil {
		b.log.Errorf("Could not store order book to database: %v", err)
	}

	return nil
}

func (b *Worker) updateCandlestick(symbol, interval string, event *binance.WsKlineEvent) error {
	if err := b.database.StoreCandlestickBinance(symbol, interval, event); err != nil {
		b.log.Errorf("Could not store candlestick to database: %v", err)
	}

	return nil
}

func (b *Worker) updateCandlestickAPI(symbol, interval string, candlestick *binance.Kline) error {
	if err := b.database.StoreCandlestickBinanceAPI(symbol, interval, candlestick); err != nil {
		b.log.Errorf("Could not store candlestick from REST API to database: %v", err)
	}

	return nil
}

func (b *Worker) StopAll() {
	for _, c := range b.stops {
		c <- struct{}{}
	}

	for _, c := range b.dones {
		<-c
	}

	b.StopC <- struct{}{}
}

func (b *Worker) makeErrorHandler() binance.ErrHandler {
	return func(err error) {
		b.log.Printf("Error in WS connection with Binance: %v", err)
	}
}

func (b *Worker) fillSymbolList() error {
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

func (b *Worker) fillSymbolListWithTestData() error {
	b.symbols = models.BinanceSymbols
	return nil
}

func (b *Worker) getOrderBook(symbol string, depth int) (response models.OrderBookInternal, err error) {
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

func (b *Worker) makeOrderBookURL(symbol string, depth int) (string, error) {
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
