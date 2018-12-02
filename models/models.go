package models

import (
	"sort"
	"strconv"

	"github.com/adshao/go-binance"
)

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
