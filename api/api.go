package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/batonych/tradingbot/logger"
	"github.com/batonych/tradingbot/storage"
	"github.com/gorilla/mux"
)

const (
	v1Prefix = "/api/v1"
)

// Config represents an API configuration.
type Config struct {
	Port int `json:"port"`
}

// API represents a REST API server instance.
type API struct {
	config  *Config
	log     *logger.Logger
	storage *storage.Client
}

// New returns a new API instance.
func New(config *Config, log *logger.Logger, storage *storage.Client) *API {
	api := &API{
		config:  config,
		log:     log,
		storage: storage,
	}

	return api
}

// Serve starts the API server.
func (api *API) Serve() error {
	api.log.Infof("Starting API")

	r := mux.NewRouter()
	s := r.PathPrefix(v1Prefix).Subrouter()

	s.HandleFunc("/orderBook", api.handleOrderBookRequest).Methods("GET")

	if err := http.ListenAndServe(":"+strconv.Itoa(api.config.Port), r); err != nil {
		return err
	}

	return nil
}

func (api *API) handleOrderBookRequest(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()

	pair, ok := vars["symbol"]
	if !ok || len(pair) == 0 {
		http.Error(w, "no pair specified", http.StatusBadRequest)
		return
	}

	ob, err := api.storage.LoadOrderBook(pair[0])
	if err != nil {
		api.log.Errorf("Could not load order book from database: %v", err)
		http.Error(w, "could not load order book", http.StatusInternalServerError)
		return
	}

	data, err := json.Marshal(ob)
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
