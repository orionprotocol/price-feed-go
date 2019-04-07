package api

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"price-feed/exchanges/binance"
	"price-feed/exchanges/bittrex"
	"price-feed/exchanges/poloniex"
	"price-feed/logger"
	"price-feed/storage"
)

const (
	v1Prefix = "/api/v1"
)

// Config represents an API configuration.
type Config struct {
	Port  int    `json:"port"`
	Token string `json:"token"`
}

// API represents a REST API server instance.
type API struct {
	config   *Config
	log      *logger.Logger
	storage  *storage.Client
	binance  *binance.Worker
	bittrex  *bittrex.Worker
	poloniex *poloniex.Worker
}

// New returns a new API instance.
func New(config *Config, log *logger.Logger, storage *storage.Client,
	binance *binance.Worker, bittrex *bittrex.Worker, poloniex *poloniex.Worker) *API {

	api := &API{
		config:   config,
		log:      log,
		storage:  storage,
		binance:  binance,
		bittrex:  bittrex,
		poloniex: poloniex,
	}

	return api
}

// Start starts the API server.
func (api *API) Start() error {
	api.log.Infof("Starting API")

	r := mux.NewRouter()
	s := r.PathPrefix(v1Prefix).Subrouter()

	s.HandleFunc("/orderBook", api.handleOrderBookRequest).Methods("GET")
	s.HandleFunc("/candles", api.handleCandlestickRequest).Methods("GET")
	s.HandleFunc("/reload", api.handleReloadRequest).Methods("GET")

	return http.ListenAndServe(":"+strconv.Itoa(api.config.Port), r)
}
