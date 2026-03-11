package web

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/klever-io/klever-subscriber/subscriber"
	"github.com/klever-io/klever-subscriber/web/broker"
	"github.com/klever-io/klever-subscriber/web/handler"
	"github.com/klever-io/klever-subscriber/web/middleware"
)

//go:embed static
var staticFiles embed.FS

type Server struct {
	addr   string
	sub    *subscriber.Subscriber
	broker *broker.Broker
}

func NewServer(addr string, sub *subscriber.Subscriber) *Server {
	return &Server{
		addr:   addr,
		sub:    sub,
		broker: broker.New(broker.DefaultMaxClients, sub.Connected),
	}
}

func (s *Server) Start(ctx context.Context) error {
	subscription := s.sub.Subscribe()
	go s.broker.ProcessEvents(ctx, subscription.C())

	staticFS, _ := fs.Sub(staticFiles, "static")

	qh := handler.NewQueryHandler(s.sub)
	sh := handler.NewSubscriptionHandler(s.sub)

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(staticFS)))
	mux.HandleFunc("/events", s.broker.HandleSSE)
	mux.HandleFunc("/stats", s.broker.HandleStats)
	mux.HandleFunc("/subscription", sh.HandleSubscription)
	mux.HandleFunc("/subscription/subscribe", sh.HandleDynamicSubscribe)
	mux.HandleFunc("/subscription/unsubscribe", sh.HandleDynamicUnsubscribe)
	mux.HandleFunc("/api/transaction", qh.HandleGetTransaction)
	mux.HandleFunc("/api/block", qh.HandleGetBlock)

	srv := &http.Server{
		Addr:              s.addr,
		Handler:           middleware.SecurityHeaders(mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		<-ctx.Done()
		subscription.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	err := srv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("web server: %w", err)
	}
	return nil
}
