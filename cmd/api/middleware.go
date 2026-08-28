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
	// gracefulShutdown coordinates the complete shutdown lifecycle. Ctrl+C sends
	// SIGINT, causing srv.Shutdown to stop accepting new requests and wait for
	// active handlers within the 30-second context. Only after the request
	// lifetime is drained does workerCancel close the worker context. The worker
	// receives ctx.Done, returns from its select or cancellable timer, and runs
	// defer app.wg.Done. Waiting after cancellation is essential: wg.Wait joins
	// the goroutine before the process exits instead of racing with it. A deferred
	// cancelWorker in main alone would run too late to coordinate this sequence.
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
