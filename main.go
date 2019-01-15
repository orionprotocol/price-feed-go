package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/batonych/tradingbot/api"
	"github.com/batonych/tradingbot/config"
	"github.com/batonych/tradingbot/exchanges/binance"
	"github.com/batonych/tradingbot/logger"
	"github.com/batonych/tradingbot/storage"
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

	binanceWorker, err := binance.NewWorker(cfg.Binance, l, database, quit)
	if err != nil {
		l.Fatalf("Could not connect to Binance: %v", err)
	}

	binanceWorker.Start()

	apiServer := api.New(cfg.API, l, database, binanceWorker)

	go func() {
		if err = apiServer.Start(); err != nil {
			l.Fatalf("Server error: %v", err)
		}
	}()

	<-quit
}
