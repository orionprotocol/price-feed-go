package api

import (
	"net/http"
	"strconv"

	"github.com/nkryuchkov/tradingbot/logger"
	"github.com/nkryuchkov/tradingbot/storage"
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

	http.HandleFunc("/", api.handleRequest)

	if err := http.ListenAndServe(":"+strconv.Itoa(api.config.Port), nil); err != nil {
		return err
	}

	return nil
}

func (api *API) handleRequest(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not implemented yet", http.StatusMethodNotAllowed)
	return
}
