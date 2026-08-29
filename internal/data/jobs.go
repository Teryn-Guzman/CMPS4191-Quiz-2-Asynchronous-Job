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

// Q11: ConsumerID identifies the consumer in the job, JobType identifies the
// task class, Status shows lifecycle stage, Payload stores the report window,
// Result stores successful JSON, ErrorMessage stores failure detail, and the
// timestamps track when the job was created, started, and completed.
// Q12: Pointers are used for StartedAt and CompletedAt because those fields are
// absent until a state transition occurs, and an absent error message indicates
// that the job did not fail.
// Q13: The payload is marshaled to JSON before insertion because the jobs table
// stores it in a jsonb column and the worker needs to read the same input later.
// Q14: The application supplies consumer_id, job_type, and payload while the
// database supplies the generated id, public_id, default status, and created_at.
// Q15: RETURNING writes the database-generated values back into the Go struct so
// the handler can immediately return the public job ID and queued status.
// Q16: A foreign-key violation means the job references a consumer that does not
// exist, so the job cannot be inserted without a valid consumer row.
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
	// Q17: The database transaction ensures the claim and state transition happen
	// atomically before any worker starts the report.
	// Q18: WHERE status = 'queued' AND job_type = 'consumer_activity_report'
	// restricts work to eligible jobs that are waiting and of the correct type.
	// Q19: ORDER BY created_at usually picks the oldest queued job, and LIMIT 1
	// guarantees a single claim per attempt.
	// Q20: FOR UPDATE locks the selected row while SKIP LOCKED makes other workers
	// skip it instead of blocking on it.
	// Q21: Selecting and updating within the same transaction prevents two workers
	// from claiming the same queued job simultaneously.
	// Q22: When no job is queued, the query returns sql.ErrNoRows, which is a
	// normal idle condition rather than a worker failure.
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
	// Q40: The generated report is marshaled before storing it so the result is
	// persisted as JSON in the jobs table.
	// Q41: MarkCompleted updates status to completed, stores the result JSON, and
	// records completed_at to mark the final success state.
	_, err := m.DB.ExecContext(ctx,
		`UPDATE jobs SET status = 'completed', result = $2, completed_at = now() WHERE id = $1`,
		id, result)
	return err
}

func (m JobModel) MarkFailed(ctx context.Context, id, message string) error {
	// Q42: The original POST may still succeed while the worker later fails, so
	// this method records the durable error state after the fact.
	// Q43: Stored error_message and logs make it possible to diagnose a failed job
	// later by calling GET /v1/jobs/{id} and inspecting the recorded failure.
	_, err := m.DB.ExecContext(ctx,
		`UPDATE jobs SET status = 'failed', error_message = $2, completed_at = now() WHERE id = $1`,
		id, message)
	return err
}
