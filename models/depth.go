package models

import (
	"github.com/adshao/go-binance"
)

// OrderBook represents the order book data format.
type OrderBook struct {
	Asks []AskBid `json:"asks"`
	Bids []AskBid `json:"bids"`
}

type AskBid struct {
	Size  string `json:"size"`
	Price string `json:"price"`
}

var EmptyOrderBook = OrderBook{
	Asks: make([]AskBid, 0),
	Bids: make([]AskBid, 0),
}

func SerializeBinanceOrderBook(event *binance.WsDepthEvent) *OrderBook {
	if event == nil {
		return nil
	}

	asks := make([]AskBid, 0)
	bids := make([]AskBid, 0)

	for _, ask := range event.Asks {
		asks = append(asks, AskBid{
			Size:  ask.Quantity,
			Price: ask.Price,
		})
	}

	for _, bid := range event.Bids {
		bids = append(bids, AskBid{
			Size:  bid.Quantity,
			Price: bid.Price,
		})
	}

	return &OrderBook{
		Asks: asks,
		Bids: bids,
	}
}
