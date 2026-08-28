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

// Job is the durable contract between the HTTP request, PostgreSQL, the worker,
// and the status endpoint. ConsumerID identifies the related consumer and
// JobType selects the work. Payload carries the report date range and is stored
// as JSON so the worker can use it after the request ends. Status records the
// queued, processing, completed, or failed stage. Result stores successful JSON
// output and ErrorMessage stores a failure reason. StartedAt and CompletedAt
// are pointers because those timestamps do not exist before their transitions;
// CreatedAt always exists after insertion. ID is the internal database key and
// is hidden from JSON, while PublicID is the opaque client lookup identifier.
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
	// ClaimNext owns the queued-to-processing transition. It begins a database
	// transaction before selecting one eligible consumer-activity job, filters
	// for status='queued' so completed or failed work cannot be repeated, and
	// orders by created_at so older work is normally considered first. LIMIT 1
	// makes each claim attempt take at most one job.
	//
	// FOR UPDATE locks the selected row until the transaction finishes, while
	// SKIP LOCKED lets another worker inspect a different row instead of waiting
	// on this one. Keeping SELECT and UPDATE in the same transaction prevents two
	// workers from both observing and claiming the same queued job. sql.ErrNoRows
	// means the queue is idle during normal polling, not that the worker failed.
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
	// MarkCompleted records the successful outcome after report generation and
	// JSON marshaling. The result is stored as jsonb, status becomes completed,
	// and completed_at records when the terminal transition occurred. A later
	// GET can therefore return the report even though POST ended earlier.
	_, err := m.DB.ExecContext(ctx,
		`UPDATE jobs SET status = 'completed', result = $2, completed_at = now() WHERE id = $1`,
		id, result)
	return err
}

func (m JobModel) MarkFailed(ctx context.Context, id, message string) error {
	// MarkFailed records failures from report generation or result marshaling.
	// It stores the diagnostic message, changes processing to failed, and sets
	// completed_at to mark the end of this attempt. Logs provide live context,
	// while this persisted error_message remains available through GET after the
	// worker has returned the error.
	_, err := m.DB.ExecContext(ctx,
		`UPDATE jobs SET status = 'failed', error_message = $2, completed_at = now() WHERE id = $1`,
		id, message)
	return err
}
