package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

func (app *application) startReportWorker(ctx context.Context) {
	// Q23: The worker starts once in main for the whole application, not once per
	// HTTP request, so the system has one queue monitor.
	// Q24: context.WithCancel creates a worker lifetime, workerCtx is that
	// context, cancelWorker stops it, and app.workerCancel stores the function for
	// shutdown.
	// Q25: app.wg.Add(1) is called before launching the goroutine so the wait
	// group can track the worker while it is still alive.
	// Q26: The goroutine runs the polling loop, and defer app.wg.Done() ensures
	// the wait group decrements when the worker exits.
	// Q27: The ticker creates periodic polling events at workerPollInterval.
	// Q28: The select listens for either ctx.Done() or ticker.C and chooses the
	// first event that arrives.
	// Q29: A longer poll interval increases the delay before a newly queued job is
	// noticed and started by the worker.
	// Q30: deferring ticker.Stop() ensures the ticker is cleaned up when the loop
	// exits.
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
	// Q31: reportDelay is applied in processNextReportJob rather than the POST
	// handler so acknowledgement time stays fast while completion time reflects the
	// artificial work delay.
	// Q32: time.NewTimer creates the delay, timer.C is the completion event, and
	// ctx.Done() can interrupt the wait early.
	// Q33: Cancellation allows the worker to stop waiting before the timer expires,
	// so shutdown can interrupt the simulated delay.
	// Q34: Increasing reportDelay affects completion time more strongly than
	// acknowledgement time because only the worker waits for the artificial delay.
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
