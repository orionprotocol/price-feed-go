package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/batonych/tradingbot/models"
)

func (api *API) handleCandlestickRequest(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()

	symbols, ok := vars["symbol"]
	if !ok || len(symbols) == 0 {
		http.Error(w, "no pair specified", http.StatusBadRequest)
		return
	}
	symbol := symbols[0]

	intervals, ok := vars["interval"]
	if !ok || len(intervals) == 0 {
		http.Error(w, "no interval specified", http.StatusBadRequest)
		return
	}
	interval := intervals[0]

	if !models.IsValidInterval(interval) {
		http.Error(w, "interval is invalid", http.StatusBadRequest)
		return
	}

	timeStarts, ok := vars["timeStart"]
	if !ok || len(timeStarts) == 0 {
		http.Error(w, "no timeStart specified", http.StatusBadRequest)
		return
	}
	timeStartStr := timeStarts[0]
	timeStart, err := strconv.ParseInt(timeStartStr, 10, 64)
	if err != nil {
		http.Error(w, "timeStart is not a number", http.StatusBadRequest)
		return
	}

	timeStart *= 1000

	timeEnds, ok := vars["timeEnd"]
	if !ok || len(timeEnds) == 0 {
		http.Error(w, "no timeEnd specified", http.StatusBadRequest)
		return
	}
	timeEndStr := timeEnds[0]
	timeEnd, err := strconv.ParseInt(timeEndStr, 10, 64)
	if err != nil {
		http.Error(w, "timeEnd is not a number", http.StatusBadRequest)
		return
	}

	timeEnd *= 1000

	candles, err := api.storage.LoadCandlestickList(symbol, interval, timeStart, timeEnd)
	if err != nil {
		http.Error(w, "no pair specified", http.StatusBadRequest)
		return
	}

	response := models.CandlestickResponse{
		TimeStart: timeStart,
		TimeEnd:   timeEnd,
		Candles:   candles,
	}

	data, err := json.Marshal(response)
	if err != nil {
		api.log.Errorf("Could not marshal json: %v", err)
		http.Error(w, "could not load candles", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	if _, err = w.Write(data); err != nil {
		api.log.Errorf("Could not write response: %v", err)
		return
	}
}
