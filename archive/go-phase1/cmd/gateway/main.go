// Package main is the entry point for the Spark Connect Gateway.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/liangchi-hsieh/spark-connect-gateway/internal/config"
	"github.com/liangchi-hsieh/spark-connect-gateway/internal/pool/static"
	"github.com/liangchi-hsieh/spark-connect-gateway/internal/proxy"
	"github.com/liangchi-hsieh/spark-connect-gateway/internal/routing"
	"github.com/liangchi-hsieh/spark-connect-gateway/internal/store/memory"
)

var version = "dev"

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to gateway config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Error("config load failed", "err", err)
		os.Exit(1)
	}

	pool, err := static.New(cfg.Backends)
	if err != nil {
		log.Error("pool init failed", "err", err)
		os.Exit(1)
	}
	router := routing.New(pool, memory.New())
	server := proxy.NewServer(router, log)

	lis, err := net.Listen("tcp", cfg.BindAddr)
	if err != nil {
		log.Error("listen failed", "addr", cfg.BindAddr, "err", err)
		os.Exit(1)
	}

	log.Info("spark-connect-gateway starting",
		"version", version,
		"addr", cfg.BindAddr,
		"backends", cfg.Backends)

	// Graceful shutdown on SIGINT / SIGTERM.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		log.Info("shutdown signal received, draining")
		server.Stop()
	}()

	if err := server.Serve(lis); err != nil {
		log.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
