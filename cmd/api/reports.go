package main

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/data"
	"github.com/teryn-guzman/gatekeeper-asynchronous/internal/validator"
)

// Q01: If the report was generated inside the HTTP request, the client would
// have to wait for all that work to finish before getting a response. That
// would make the API slower and could eventually cause a timeout.

// Q02: I understand the requirement as accepting the request quickly, then
// allowing the expensive report generation to happen separately in the worker.

// Q03: The HTTP request only lasts until the POST returns 202. The actual job
// has its own lifetime, starting when the worker picks it up and ending when
// the job is completed or fails.

// Q04: Because the request is already finished, the application needs to save
// the job in the database so the client can check its status and result later.

// Q05: POST /v1/reports is basically telling the server to accept this report
// request for processing. A 202 response tells the client that the request was
// accepted, not that the report is already finished.

// Q06: Getting a 202 does not guarantee that the report will succeed. The
// worker still has to process it, and something could go wrong during that
// process.

// Q07: The response gives the client the public job ID and status URL so it
// knows where to check the job later.

// Q08: PublicID is used instead of exposing the database ID because the client
// only needs an external identifier. This also keeps the database's internal
// ID separate from what the API exposes.

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

// Q09: GET /v1/jobs/{id} does not create or start another job. It simply looks
// up the existing job and returns its current status and result from the database.

// Q10: Queued means the job is waiting for the worker, processing means the
// worker has picked it up, completed means the report finished successfully,
// and failed means something went wrong while processing it.
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
