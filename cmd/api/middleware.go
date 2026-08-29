package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func (app *application) logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		app.logger.Info("request started", "method", r.Method, "uri", r.URL.RequestURI())
		next.ServeHTTP(w, r)
		app.logger.Info("request completed", "method", r.Method, "uri", r.URL.RequestURI(), "duration", time.Since(start).String())
	})
}

func (app *application) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				w.Header().Set("Connection", "close")
				app.serverErrorResponse(w, r, fmt.Errorf("%v", err))
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func (app *application) gracefulShutdown(srv *http.Server) <-chan error {
	// Q45: gracefulShutdown waits for the signal, calls srv.Shutdown, and then
	// cancels the worker so active HTTP requests drain before background work is
	// stopped.
	// Q46: The worker is cancelled before waiting so its polling loop sees the
	// context cancellation and exits cleanly.
	// Q47: The defer in main only runs at process exit and cannot coordinate the
	// shutdown sequence while the server is still running.
	// Q48: app.wg.Wait waits until the worker goroutine has executed its defer and
	// called app.wg.Done, which ensures a clean shutdown without a race.
	shutdownError := make(chan error, 1)

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(quit)

		s := <-quit
		app.logger.Info("caught signal", "signal", s.String())

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			shutdownError <- err
			return
		}

		app.logger.Info("completing background tasks", "addr", srv.Addr)
		if app.workerCancel != nil {
			app.workerCancel()
		}
		app.wg.Wait()
		shutdownError <- nil
	}()

	return shutdownError
}
