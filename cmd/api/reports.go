package main

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/data"
	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/validator"
)

// Q01: Doing work inside the original HTTP request would block the client until
// the report finishes, which makes acknowledgement latency rise and can exceed
// the request timeout.
// Q02: The requirement is to accept valid work quickly while letting expensive
// report generation continue independently in a background worker.
// Q03: The request lifetime ends when the POST returns 202, while the work
// lifetime begins when the worker claims the job and ends when the job reaches a
// terminal state.
// Q04: The application needs a persistent job resource so the client can later
// fetch the job's state and result after the original request has already ended.
// Q05: POST /v1/reports asks the server to accept a report command for later
// processing, and HTTP 202 Accepted promises only that the work was accepted.
// Q06: 202 Accepted does not mean the report finished or that it will succeed;
// the worker may still fail later during generation or marshaling.
// Q07: The response must include the public job ID and a status URL so the
// client can locate the job later with GET /v1/jobs/{id}.
// Q08: PublicID is exposed instead of the internal database ID because the
// client needs a stable external identifier without depending on DB internals.
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

// Q09: GET /v1/jobs/{id} is a status lookup, not a new work submission; it
// reads the job's current state and result from PostgreSQL.
// Q10: queued means accepted but not yet claimed, processing means the worker
// has claimed it, completed means the report was generated and stored, and
// failed means the job ended with an error message.
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
