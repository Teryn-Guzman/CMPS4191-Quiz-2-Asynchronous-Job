package data

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/lib/pq"
)

type ReportPayload struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// Q11: These fields describe the job from start to finish. ConsumerID tells me
// who the job belongs to, JobType tells me what work needs to be done, Status
// tells me where it is in the process, Payload contains the input, and Result
// or ErrorMessage stores what happened. The timestamps help track when each
// stage occurred.

// Q12: StartedAt and CompletedAt are pointers because a new job does not have
// those times yet. They only get values when the worker starts and finishes the
// job.

// Q13: The payload is converted to JSON before inserting it because the
// database stores it in a jsonb column. This lets the worker retrieve the same
// information later when it processes the job.

// Q14: The application provides the information about the job, while the
// database generates values such as the ID, public ID, default status, and
// creation time.

// Q15: RETURNING gives the application the values that PostgreSQL generated.
// That means the handler immediately knows the public job ID and current status
// to send back to the client.

// Q16: A foreign-key violation means the consumer being referenced does not
// exist. Since the job must belong to a valid consumer, PostgreSQL rejects it.

type Job struct {
	ID           string          `json:"-"`
	PublicID     string          `json:"id"`
	ConsumerID   string          `json:"consumer_id"`
	JobType      string          `json:"job_type"`
	Status       string          `json:"status"`
	Payload      ReportPayload   `json:"payload"`
	Result       json.RawMessage `json:"result,omitempty"`
	ErrorMessage *string         `json:"error_message,omitempty"`
	StartedAt    *time.Time      `json:"started_at,omitempty"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

type JobModel struct {
	DB *sql.DB
}

func (m JobModel) Insert(job *Job) error {
	// JSON lets one durable payload column carry the report's date range across
	// the request/worker boundary without keeping the HTTP request alive.
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return err
	}
	query := `INSERT INTO jobs (consumer_id, job_type, payload)
		VALUES ($1, $2, $3) RETURNING id, public_id, status, created_at`
	// RETURNING fills the in-memory job with database-generated IDs, the queued
	// default, and its creation time before the 202 response is written.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = m.DB.QueryRowContext(ctx, query, job.ConsumerID, job.JobType, payload).Scan(
		&job.ID, &job.PublicID, &job.Status, &job.CreatedAt,
	)
	if err != nil {
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return ErrRecordNotFound
		}
		return err
	}
	return nil
}

func (m JobModel) GetByPublicID(publicID string) (*Job, error) {
	// Public IDs are the external lookup key; the internal UUID is retained for
	// database updates but is never serialized in the API response.
	query := `SELECT id, public_id, consumer_id, job_type, status, payload,
		COALESCE(result, 'null'::jsonb), error_message, started_at, completed_at, created_at
		FROM jobs WHERE public_id = $1`
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var job Job
	var payload []byte
	err := m.DB.QueryRowContext(ctx, query, publicID).Scan(&job.ID, &job.PublicID,
		&job.ConsumerID, &job.JobType, &job.Status, &payload, &job.Result,
		&job.ErrorMessage, &job.StartedAt, &job.CompletedAt, &job.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}
	if err := json.Unmarshal(payload, &job.Payload); err != nil {
		return nil, err
	}
	return &job, nil
}

func (m JobModel) ClaimNext(ctx context.Context) (*Job, error) {
	
	// Q17: The transaction keeps the process of finding and claiming a job together.
	// The worker should not start processing a job until the database has successfully
	// changed its state.

	// Q18: These conditions make sure the worker only picks jobs that are still
	// queued and are the specific type of report this worker knows how to process.

	// Q19: ORDER BY created_at normally gives me the oldest waiting job first, while
	// LIMIT 1 makes sure one worker attempt only claims one job.

	// Q20: FOR UPDATE locks the selected job while it is being claimed. SKIP LOCKED
	// lets another worker skip that locked job instead of waiting for it.

	// Q21: Because selecting and updating happen inside the same transaction, two
	// workers cannot successfully claim the same queued job at the same time.

	// Q22: If there are no queued jobs, sql.ErrNoRows simply tells the worker that
	// there is currently nothing to process. That is normal and not an error that
	// should stop the worker.

	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	query := `SELECT id, public_id, consumer_id, job_type, payload FROM jobs
		WHERE status = 'queued' AND job_type = 'consumer_activity_report'
		ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1`
	var job Job
	var payload []byte
	if err := tx.QueryRowContext(ctx, query).Scan(&job.ID, &job.PublicID,
		&job.ConsumerID, &job.JobType, &payload); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(payload, &job.Payload); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE jobs SET status = 'processing', started_at = now() WHERE id = $1`, job.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	job.Status = "processing"
	return &job, nil
}

func (m JobModel) MarkCompleted(ctx context.Context, id string, result []byte) error {

	// Q40: The generated report is converted to JSON before being saved because
	// the result needs to be stored in the database's JSON field.

	// Q41: MarkCompleted changes the job to completed, saves the report result, and
	// records the completion time. This gives the job a final successful state.

	_, err := m.DB.ExecContext(ctx,
		`UPDATE jobs SET status = 'completed', result = $2, completed_at = now() WHERE id = $1`,
		id, result)
	return err
}

func (m JobModel) MarkFailed(ctx context.Context, id, message string) error {
	
	// Q42: The POST request can already have returned successfully by the time the
	// worker finds an error. MarkFailed lets the worker save that failure to the
	// database after the original request has ended.

	// Q43: Saving the error message means I can find out what happened later instead
	// of losing the error when the worker finishes. The client can see that failure
	// through GET /v1/jobs/{id}.

	_, err := m.DB.ExecContext(ctx,
		`UPDATE jobs SET status = 'failed', error_message = $2, completed_at = now() WHERE id = $1`,
		id, message)
	return err
}
