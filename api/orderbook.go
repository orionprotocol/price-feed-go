package api

import (
	"encoding/json"
	"net/http"

	"github.com/batonych/tradingbot/models"
)

type orderBookResponse struct {
	Symbol string `json:"symbol"`
	models.OrderBook
}

func (api *API) handleOrderBookRequest(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()

	symbols, ok := vars["symbol"]
	if !ok || len(symbols) == 0 {
		http.Error(w, "no pair specified", http.StatusBadRequest)
		return
	}
	symbol := symbols[0]

	orderBook, err := api.storage.LoadOrderBook(symbol)
	if err != nil {
		api.log.Errorf("Could not load order book from database: %v", err)
		http.Error(w, "could not load order book", http.StatusInternalServerError)
		return
	}

	resp := orderBookResponse{
		Symbol:    symbol,
		OrderBook: orderBook,
	}

	data, err := json.Marshal(resp)
	if err != nil {
		api.log.Errorf("Could not marshal json: %v", err)
		http.Error(w, "could not load order book", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err = w.Write(data); err != nil {
		api.log.Errorf("Could not write response: %v", err)
		return
	}
}
