package main

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/data"
	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/validator"
)

// createReportHandler accepts a report command without performing expensive
// report generation inside the original HTTP request. Synchronous generation
// would keep the client waiting during the artificial delay and database query,
// possibly exceeding the HTTP timeout. Instead, this handler validates the
// consumer and date range, creates a durable queued job, and ends the request
// with HTTP 202 Accepted. 202 confirms that work was accepted for later
// processing; it does not promise completion or success.
//
// Insert returns an opaque PublicID and the initial queued status. PublicID is
// the external identifier rather than the internal database ID, so clients do
// not depend on database implementation details. The status URL lets a client
// make a later GET request after the request lifetime ends and observe the
// PostgreSQL-owned state. The worker lifetime begins separately when it claims
// this job.
func (app *application) createReportHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ConsumerID string    `json:"consumer_id"`
		From       time.Time `json:"from"`
		To         time.Time `json:"to"`
	}
	if err := app.readJSON(w, r, &input); err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	v := validator.New()
	v.Check(input.ConsumerID != "", "consumer_id", "must be provided")
	v.Check(!input.From.IsZero(), "from", "must be provided")
	v.Check(!input.To.IsZero(), "to", "must be provided")
	v.Check(input.From.Before(input.To), "from", "must be earlier than to")
	if !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	job := &data.Job{
		ConsumerID: input.ConsumerID,
		JobType:    "consumer_activity_report",
		Payload:    data.ReportPayload{From: input.From, To: input.To},
	}
	// Insert owns the durable acceptance boundary: PostgreSQL creates the
	// queued state and identifiers, while the worker will consume the payload
	// after this HTTP request has finished.
	if err := app.models.Jobs.Insert(job); err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
			return
		}
		app.serverErrorResponse(w, r, err)
		return
	}

	statusURL := fmt.Sprintf("/v1/jobs/%s", job.PublicID)
	// 202 means the command was accepted for later processing, not that the
	// report already exists. The public ID and URL give the client a stable
	// resource to inspect after the request ends.
	headers := make(http.Header)
	headers.Set("Location", statusURL)
	response := envelope{"job_id": job.PublicID, "status": job.Status, "status_url": statusURL}
	if err := app.writeJSON(w, http.StatusAccepted, response, headers); err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

// getJobHandler observes the persistent job resource. Unlike POST, it neither
// accepts new work nor executes the report: it reads the current queued,
// processing, completed, or failed state recorded by the data layer. A
// completed job includes its stored JSON result, while a failed job exposes its
// stored error message, so a POST that accepted work can still be diagnosed if
// background execution later fails.
func (app *application) getJobHandler(w http.ResponseWriter, r *http.Request) {
	job, err := app.models.Jobs.GetByPublicID(r.PathValue("id"))
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
			return
		}
		app.serverErrorResponse(w, r, err)
		return
	}
	if err := app.writeJSON(w, http.StatusOK, envelope{"job": job}, nil); err != nil {
		app.serverErrorResponse(w, r, err)
	}
}
