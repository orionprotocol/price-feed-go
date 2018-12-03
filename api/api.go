package api

import (
	"net/http"
	"strconv"

	"github.com/batonych/tradingbot/exchanges/binance"
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
	binance *binance.OrderBook
}

// New returns a new API instance.
func New(config *Config, log *logger.Logger, storage *storage.Client, binance *binance.OrderBook) *API {
	api := &API{
		config:  config,
		log:     log,
		storage: storage,
		binance: binance,
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
