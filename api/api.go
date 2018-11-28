package api

import (
	"net/http"
	"strconv"

	"github.com/nkryuchkov/tradingbot/logger"
)

type Config struct {
	Port int `json:"port"`
}

type API struct {
	config *Config
	log    *logger.Logger
}

func New(config *Config, log *logger.Logger) *API {
	api := &API{
		config: config,
		log:    log,
	}

	return api
}

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
