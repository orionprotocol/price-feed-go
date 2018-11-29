package main

import (
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/nkryuchkov/tradingbot/orderbook/binance"

	"github.com/nkryuchkov/tradingbot/api"
	"github.com/nkryuchkov/tradingbot/config"
	"github.com/nkryuchkov/tradingbot/logger"
	"github.com/nkryuchkov/tradingbot/storage"
)

var (
	wsTimeoutStr = "12h"
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

	database := storage.New(cfg.Storage)
	pong, err := database.Check()
	if err != nil {
		l.Fatalf("Can't establish connection to database: %v", err)
	}
	l.Infof("Database check reply: %v", pong)

	wsTimeout, err := time.ParseDuration(wsTimeoutStr)
	if err != nil {
		l.Fatalf("Couldn't parse WS timeout: %v\n", err)
	}

	symbols, err := binance.GetSymbols()
	if err != nil {
		l.Fatalf("Couldn't get symbols: %v\n", err)
	}

	binanceOrderBook := binance.New()
	for _, symbol := range symbols {
		err := binanceOrderBook.DiffDepths(symbol)
		if err != nil {
			l.Printf("Couldn't get diff depths on symbol %s: %v\n", symbol, err)
		}
	}

	for {
		go func() {
			for {
				select {
				case <-binanceOrderBook.StopC:
					return
				case depth := <-binanceOrderBook.DiffDepthsC:
					database.Store("depth", float64(time.Now().Unix()), depth)
				}
			}
		}()

		time.Sleep(wsTimeout)

		binanceOrderBook.StopAll()
	}

	server := api.New(cfg.API, l, database)

	go func() {
		if err = server.Serve(); err != nil {
			l.Fatalf("Server error: %v", err)
		}
	}()

	<-quit

	binanceOrderBook.StopAll()
}
