package models

import (
	"sort"
	"strconv"
	"time"

	"github.com/toorop/go-bittrex"

	"github.com/adshao/go-binance"
)

var (
	BinanceCandlestickIntervalList = []string{
		"1m",
		"3m",
		"5m",
		"15m",
		"30m",
		"1h",
		"2h",
		"4h",
		"6h",
		"8h",
		"12h",
		"1d",
		"3d",
		"1w",
		"1M",
	}

	BittrexCandlestickIntervalList = []string{
		"oneMin", "fiveMin", "thirtyMin", "hour", "day",
	}
)

func BittrexIntervalToBinance(v string) string {
	switch v {
	case "oneMin":
		return "1m"
	case "fiveMin":
		return "5m"
	case "thirtyMin":
		return "30m"
	case "hour":
		return "1h"
	case "day":
		return "1d"
	}

	return ""
}

func IsValidInterval(s string) bool {
	for _, v := range BinanceCandlestickIntervalList {
		if v == s {
			return true
		}
	}
	return false
}

// OrderBookAPI represents the order book data format.
type OrderBookAPI struct {
	Asks []AskBid `json:"asks"`
	Bids []AskBid `json:"bids"`
}

type AskBid struct {
	Size  float64 `json:"size"`
	Price float64 `json:"price"`
}

var EmptyOrderBook = OrderBookAPI{
	Asks: make([]AskBid, 0),
	Bids: make([]AskBid, 0),
}

type OrderBookInternal struct {
	LastUpdateID int64             `json:"-"`
	Bids         map[string]string `json:"bids"`
	Asks         map[string]string `json:"asks"`
}

func (obi *OrderBookInternal) Format(depth int) OrderBookAPI {
	asks := make([]AskBid, 0, len(obi.Asks))
	for k, v := range obi.Asks {
		price, err := strconv.ParseFloat(k, 64)
		if err != nil {
			continue
		}

		size, err := strconv.ParseFloat(v, 64)
		if err != nil {
			continue
		}

		asks = append(asks, AskBid{
			Size:  size,
			Price: price,
		})
	}

	bids := make([]AskBid, 0, len(obi.Bids))
	for k, v := range obi.Bids {
		price, err := strconv.ParseFloat(k, 64)
		if err != nil {
			continue
		}

		size, err := strconv.ParseFloat(v, 64)
		if err != nil {
			continue
		}

		bids = append(bids, AskBid{
			Size:  size,
			Price: price,
		})
	}

	sort.Slice(asks, func(i, j int) bool {
		return asks[i].Price < asks[j].Price
	})

	sort.Slice(bids, func(i, j int) bool {
		return bids[i].Price < bids[j].Price
	})

	asksDepth := len(asks)
	if depth < asksDepth {
		asksDepth = depth
	}

	bidsDepth := len(bids)
	if depth < bidsDepth {
		bidsDepth = depth
	}

	return OrderBookAPI{
		Asks: asks[:asksDepth],
		Bids: bids[len(bids)-bidsDepth:],
	}
}

var EmptyOrderBookInternal = OrderBookInternal{
	Asks: make(map[string]string),
	Bids: make(map[string]string),
}

type OrderBookResponse struct {
	LastUpdateID int64       `json:"lastUpdateId"`
	Bids         [][2]string `json:"bids"` // price, quantity
	Asks         [][2]string `json:"asks"` // price, quantity
}

func SerializeBinanceOrderBookREST(data OrderBookResponse) OrderBookInternal {
	asks := make(map[string]string)
	bids := make(map[string]string)

	for _, ask := range data.Asks {
		asks[ask[0]] = ask[1]
	}

	for _, bid := range data.Bids {
		bids[bid[0]] = bid[1]
	}

	return OrderBookInternal{
		LastUpdateID: data.LastUpdateID,
		Asks:         asks,
		Bids:         bids,
	}
}

func SerializeBinanceOrderBookWS(event *binance.WsDepthEvent) *OrderBookAPI {
	if event == nil {
		return nil
	}

	asks := make([]AskBid, 0)
	bids := make([]AskBid, 0)

	for _, ask := range event.Asks {
		price, err := strconv.ParseFloat(ask.Price, 64)
		if err != nil {
			continue
		}

		size, err := strconv.ParseFloat(ask.Quantity, 64)
		if err != nil {
			continue
		}

		asks = append(asks, AskBid{
			Size:  size,
			Price: price,
		})
	}

	for _, bid := range event.Bids {
		price, err := strconv.ParseFloat(bid.Price, 64)
		if err != nil {
			continue
		}

		size, err := strconv.ParseFloat(bid.Quantity, 64)
		if err != nil {
			continue
		}

		bids = append(bids, AskBid{
			Size:  size,
			Price: price,
		})
	}

	return &OrderBookAPI{
		Asks: asks,
		Bids: bids,
	}
}

type CandlestickResponse struct {
	TimeStart int64    `json:"timeStart"`
	TimeEnd   int64    `json:"timeEnd"`
	Candles   []Candle `json:"candles"`
}

type Candle struct {
	TimeStart int64   `json:"timeStart"`
	TimeEnd   int64   `json:"timeEnd"`
	Time      int64   `json:"time"`
	Open      float64 `json:"open"`
	Close     float64 `json:"close"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Volume    float64 `json:"volume"`
}

func CandleFromEvent(event *binance.WsKlineEvent) *Candle {
	if event == nil {
		return nil
	}

	return &Candle{
		TimeStart: event.Kline.StartTime / 1000,
		TimeEnd:   event.Kline.EndTime / 1000,
		Time:      event.Time / 1000,
		Open:      mustParseFloat64(event.Kline.Open),
		Close:     mustParseFloat64(event.Kline.Close),
		High:      mustParseFloat64(event.Kline.High),
		Low:       mustParseFloat64(event.Kline.Low),
		Volume:    mustParseFloat64(event.Kline.Volume),
	}
}

func CandleFromBinanceAPI(candlestick *binance.Kline) *Candle {
	return &Candle{
		TimeStart: candlestick.OpenTime / 1000,
		TimeEnd:   candlestick.CloseTime / 1000,
		Time:      time.Now().Unix(),
		Open:      mustParseFloat64(candlestick.Open),
		Close:     mustParseFloat64(candlestick.Close),
		High:      mustParseFloat64(candlestick.High),
		Low:       mustParseFloat64(candlestick.Low),
		Volume:    mustParseFloat64(candlestick.Volume),
	}
}

func CandleFromBittrexAPI(candlestick *bittrex.Candle) *Candle {
	open, _ := candlestick.Open.Float64()
	close, _ := candlestick.Close.Float64()
	low, _ := candlestick.Low.Float64()
	high, _ := candlestick.High.Float64()
	volume, _ := candlestick.Volume.Float64()

	return &Candle{
		TimeStart: candlestick.TimeStamp.Unix(),
		TimeEnd:   candlestick.TimeStamp.Unix(),
		Time:      time.Now().Unix(),
		Open:      open,
		Close:     close,
		High:      high,
		Low:       low,
		Volume:    volume,
	}
}

func mustParseFloat64(s string) float64 {
	val, _ := strconv.ParseFloat(s, 64)
	return val
}

var BittrexSymbols = []string{
	"BTC-LTC", "BTC-ETH", "BTC-DASH", "BTC-ZEC", "BTC-BCH", "BTC-BSV",
	"ETH-LTC", "ETH-DASH", "ETH-ZEC",
	"USD-BTC", "USD-LTC", "USD-ETH", "USD-BCH", "USD-BSV",
}

var BinanceSymbols = []string{
	"LTCBTC", "ETHBTC", "DASHBTC", "ZECBTC", "BCHABCBTC", "BCHSVBTC",
	"LTCETH", "DASHETH", "ZECETH",
	"BTCUSDT", "LTCUSDT", "ETHUSDT", "BCHABCUSDT", "BCHSVUSDT",
}

var PoloniexSymbols = []string{
	"btc-ltc", "btc-eth", "btc-dash", "btc-zec", "btc-bch", "btc-bsv",
	"eth-ltc", "eth-dash", "eth-zec",
	"usd-btc", "usd-ltc", "usd-eth", "usd-bch", "usd-bsv",
}

func BittrexSymbolToBinance(symbol string) string {
	switch symbol {
	case "BTC-LTC":
		return "LTCBTC"
	case "BTC-ETH":
		return "ETHBTC"
	case "BTC-DASH":
		return "DASHBTC"
	case "BTC-ZEC":
		return "ZECBTC"
	case "BTC-BCH":
		return "BCHABCBTC"
	case "BTC-BSV":
		return "BCHSVBTC"
	case "ETH-LTC":
		return "LTCETH"
	case "ETH-DASH":
		return "DASHETH"
	case "ETH-ZEC":
		return "ZECETH"
	case "USD-BTC":
		return "BTCUSDT"
	case "USD-LTC":
		return "LTCUSDT"
	case "USD-ETH":
		return "ETHUSDT"
	case "USD-BCH":
		return "BCHABCUSDT"
	case "USD-BSV":
		return "BCHSVUSDT"
	}
	return ""
}
