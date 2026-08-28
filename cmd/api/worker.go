package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

func (app *application) startReportWorker(ctx context.Context) {
	// startReportWorker starts one application-level background worker rather
	// than creating a worker per HTTP request. context.WithCancel is created by
	// main; ctx represents the worker lifetime and its Done channel is the stop
	// signal. Add(1) happens before launching the goroutine so shutdown cannot
	// wait on an unregistered task, and Done signals that the goroutine exited.
	// A ticker drives queue checks at workerPollInterval. Each select chooses
	// between cancellation and the next poll; an empty queue returns
	// sql.ErrNoRows, which is normal and is intentionally not logged as failure.
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
	// processNextReportJob is where background work begins. ClaimNext durably
	// changes the job to processing before the report query runs, so PostgreSQL
	// owns the visible state while this worker owns execution. The artificial
	// reportDelay belongs here rather than in POST, which keeps acknowledgement
	// latency separate from completion latency. A timer plus select allows
	// ctx.Done to interrupt the delay during shutdown; a normal timer event then
	// permits report generation. Successful reports are marshaled and completed,
	// while query or marshaling errors are stored through MarkFailed. Cancellation
	// during the delay returns context.Canceled before either terminal update, so
	// this starter implementation can leave a claimed job in processing.
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
