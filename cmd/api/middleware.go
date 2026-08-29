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

	// Q45: gracefulShutdown first waits for the shutdown signal, then shuts down
	// the HTTP server so active requests can finish before stopping the worker.

	// Q46: The worker is cancelled before waiting because its polling loop needs
	// to receive the cancellation signal in order to exit.

	// Q47: The defer in main happens too late to control this shutdown process.
	// It only runs when main is already finishing.

	// Q48: app.wg.Wait makes sure the worker has actually stopped. The worker calls
	// app.wg.Done when it exits, which lets Wait continue and gives us a clean shutdown.
	
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
