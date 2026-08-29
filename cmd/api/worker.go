package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

func (app *application) startReportWorker(ctx context.Context) {

	// Q23: The worker is started once when the application starts. I don't want a
	// new worker for every HTTP request because the worker's job is to continuously
	// watch the queue for work.

	// Q24: context.WithCancel gives the worker its own lifetime. workerCtx is the
	// context the worker watches, cancelWorker tells it to stop, and the function
	// is saved in app so the shutdown code can use it.

	// Q25: app.wg.Add(1) happens before starting the goroutine so the wait group
	// knows about the worker before it starts running.

	// Q26: The goroutine contains the worker's polling loop. defer app.wg.Done()
	// makes sure the wait group is updated when the worker eventually exits.

	// Q27: The ticker is what makes the worker check for new jobs repeatedly instead
	// of checking the database continuously.

	// Q28: The select waits for either a shutdown signal or the next polling event.
	// Whichever one happens first determines what the worker does next.

	// Q29: If I increase the polling interval, the worker checks less often, so a
	// newly queued job could sit in the database longer before being picked up.

	// Q30: ticker.Stop() cleans up the ticker when the worker stops, so the ticker
	// does not continue running after the polling loop has ended.

	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		ticker := time.NewTicker(app.config.workerPollInterval)
		defer ticker.Stop()
		for {
			select {
			// Cancellation wins over future polling and lets the worker leave
			// without starting another job.
			case <-ctx.Done():
				app.logger.Info("report worker stopped")
				return
			case <-ticker.C:
				err := app.processNextReportJob(ctx)
				if err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, context.Canceled) {
					app.logger.Error("report worker failed", "error", err)
				}
			}
		}
	}()
}

func (app *application) processNextReportJob(ctx context.Context) error {
	
	// Q31: reportDelay is placed in processNextReportJob instead of the POST
	// handler because I want the POST request to return quickly. Only the worker
	// should experience the artificial processing delay.

	// Q32: time.NewTimer starts the delay, timer.C tells the code when the delay
	// has finished, and ctx.Done allows the wait to be interrupted if the worker
	// is being shut down.

	// Q33: Cancellation is useful here because the worker does not have to wait for
	// the full delay if the application is shutting down. The context can interrupt
	// the timer wait.

	// Q34: Increasing reportDelay mainly makes the report take longer to complete.
	// The acknowledgement time should stay mostly the same because the POST does
	// not wait for the worker to finish.

	job, err := app.models.Jobs.ClaimNext(ctx)
	if err != nil {
		return err
	}
	app.logger.Info("report job started", "job_id", job.PublicID,
		"artificial_delay", app.config.reportDelay)

	if app.config.reportDelay > 0 {
		timer := time.NewTimer(app.config.reportDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	report, err := app.models.Reports.Generate(job.ConsumerID, job.Payload.From, job.Payload.To)
	if err != nil {
		return app.models.Jobs.MarkFailed(ctx, job.ID, err.Error())
	}
	result, err := json.Marshal(report)
	if err != nil {
		return app.models.Jobs.MarkFailed(ctx, job.ID, err.Error())
	}
	if err := app.models.Jobs.MarkCompleted(ctx, job.ID, result); err != nil {
		return err
	}
	app.logger.Info("report job completed", "job_id", job.PublicID)
	return nil
}
