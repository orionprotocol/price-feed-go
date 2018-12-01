package models

import (
	"github.com/adshao/go-binance"
)

type Depth struct {
	Bids []binance.Bid `json:"bids"`
	Asks []binance.Ask `json:"asks"`
}
