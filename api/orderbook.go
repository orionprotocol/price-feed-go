package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/batonych/tradingbot/models"
)

type orderBookResponse struct {
	Symbol string `json:"symbol"`
	models.OrderBookAPI
}

type orderBookResponseInternal struct {
	Symbol string `json:"symbol"`
	models.OrderBookAPI
}

func (api *API) handleOrderBookRequest(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()

	symbols, ok := vars["symbol"]
	if !ok || len(symbols) == 0 {
		http.Error(w, "no pair specified", http.StatusBadRequest)
		return
	}
	symbol := symbols[0]

	depths, ok := vars["depth"]
	if !ok || len(depths) == 0 {
		http.Error(w, "no depth specified", http.StatusBadRequest)
		return
	}
	depthStr := depths[0]

	depth, err := strconv.Atoi(depthStr)
	if err != nil {
		http.Error(w, "depth should be a number", http.StatusBadRequest)
		return
	}

	orderBook, err := api.storage.LoadOrderBookInternal(symbol, depth)
	if err != nil {
		api.log.Errorf("Could not load order book from database: %v", err)
		http.Error(w, "could not load order book", http.StatusInternalServerError)
		return
	}

	resp := orderBookResponseInternal{
		Symbol:       symbol,
		OrderBookAPI: orderBook,
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
