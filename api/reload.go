package api

import (
	"net/http"
)

func (api *API) handleReloadRequest(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()

	tokens, ok := vars["token"]
	if !ok || len(tokens) == 0 {
		http.Error(w, "no token specified", http.StatusBadRequest)
		return
	}
	token := tokens[0]

	if token != api.config.Token {
		http.Error(w, "token is invalid", http.StatusUnauthorized)
		return
	}

	api.binance.Reload()
	api.bittrex.Reload()
	api.poloniex.Reload()

	w.WriteHeader(http.StatusOK)
}
