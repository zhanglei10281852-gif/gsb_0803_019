// Command coordinator runs the live segment publish coordination service.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gsb/coordinator/internal/httpapi"
	"github.com/gsb/coordinator/internal/service"
	"github.com/gsb/coordinator/internal/storage"
)

func main() {
	addr := os.Getenv("COORDINATOR_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	coord := service.New(storage.New())
	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(coord),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		log.Printf("coordinator listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
