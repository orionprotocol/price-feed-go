package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/nkryuchkov/tradingbot/api"
	"github.com/nkryuchkov/tradingbot/config"
	"github.com/nkryuchkov/tradingbot/exchanges/binance"
	"github.com/nkryuchkov/tradingbot/logger"
	"github.com/nkryuchkov/tradingbot/storage"
)

func main() {
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)

	cfg, err := config.FromFile()
	if err != nil {
		log.Fatalf("Could not read config: %v. Exiting", err)
	}

	l := logger.New(cfg.Logger)
	defer func() {
		if err = l.Close(); err != nil {
			log.Printf("Could not close logger: %v", err)
		}
	}()

	database := storage.New(cfg.Storage, l)
	pong, err := database.Check()
	if err != nil {
		l.Fatalf("Can't establish connection to database: %v", err)
	}
	l.Infof("Database check reply: %v", pong)

	apiServer := api.New(cfg.API, l, database)

	go func() {
		if err = apiServer.Serve(); err != nil {
			l.Fatalf("Server error: %v", err)
		}
	}()

	binanceExchange, err := binance.New(cfg.Binance, l, database, quit)
	if err != nil {
		l.Fatalf("Could not connect to Binance: %v", err)
	}

	binanceExchange.StartOrderBookWorker()

	<-quit
}
