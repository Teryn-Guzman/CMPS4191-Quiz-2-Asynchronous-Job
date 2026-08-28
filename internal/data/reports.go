package data

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type ConsumerActivityReport struct {
	ConsumerID     string    `json:"consumer_id"`
	ConsumerName   string    `json:"consumer_name"`
	From           time.Time `json:"from"`
	To             time.Time `json:"to"`
	ActiveKeys     int       `json:"active_keys"`
	RevokedKeys    int       `json:"revoked_keys"`
	QueuedJobs     int       `json:"queued_jobs"`
	ProcessingJobs int       `json:"processing_jobs"`
	CompletedJobs  int       `json:"completed_jobs"`
	FailedJobs     int       `json:"failed_jobs"`
	GeneratedAt    time.Time `json:"generated_at"`
}

type ReportModel struct {
	DB *sql.DB
}

// Generate builds the consumer activity report used by the worker. The query
// starts at consumers so the requested consumer is the parent row; LEFT JOINs
// preserve that row even when it has no API keys or jobs. Joining both child
// tables can multiply rows, so COUNT(DISTINCT k.id) and COUNT(DISTINCT j.id)
// prevent inflated totals. FILTER (WHERE ...) calculates separate counts for
// each key or job status from the same grouped result.
//
// Jobs use the half-open interval created_at >= from and created_at < to.
// That includes the start boundary but excludes the end boundary, preventing
// adjacent report windows from counting the same job twice. GROUP BY c.id,
// c.name produces one aggregate row for the consumer, and Scan copies that row
// into ConsumerActivityReport. QueryRowContext returning no rows means the
// requested consumer does not exist; other errors are actual report failures.
func (m ReportModel) Generate(consumerID string, from, to time.Time) (*ConsumerActivityReport, error) {
	query := `
		SELECT c.id, c.name,
			COUNT(DISTINCT k.id) FILTER (WHERE k.status = 'active'),
			COUNT(DISTINCT k.id) FILTER (WHERE k.status = 'revoked'),
			COUNT(DISTINCT j.id) FILTER (WHERE j.status = 'queued'),
			COUNT(DISTINCT j.id) FILTER (WHERE j.status = 'processing'),
			COUNT(DISTINCT j.id) FILTER (WHERE j.status = 'completed'),
			COUNT(DISTINCT j.id) FILTER (WHERE j.status = 'failed')
		FROM consumers c
		LEFT JOIN api_keys k ON k.consumer_id = c.id
		LEFT JOIN jobs j ON j.consumer_id = c.id
			AND j.created_at >= $2 AND j.created_at < $3
		WHERE c.id = $1
		GROUP BY c.id, c.name`
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	report := &ConsumerActivityReport{From: from, To: to, GeneratedAt: time.Now()}
	err := m.DB.QueryRowContext(ctx, query, consumerID, from, to).Scan(
		&report.ConsumerID, &report.ConsumerName, &report.ActiveKeys, &report.RevokedKeys,
		&report.QueuedJobs, &report.ProcessingJobs, &report.CompletedJobs, &report.FailedJobs,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}
	return report, nil
}
