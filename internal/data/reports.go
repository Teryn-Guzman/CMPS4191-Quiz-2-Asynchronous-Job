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

// Q35: The query starts with consumers and uses LEFT JOIN because I still want
// to see a consumer even if they do not have any API keys or jobs.

// Q36: Joining multiple tables can create duplicate combinations of rows. Using
// COUNT(DISTINCT ...) prevents the same key or job from being counted more than
// once.

// Q37: FILTER lets the query count only rows that match a particular status.
// This gives me separate totals instead of having to run a different query for
// every status.

// Q38: The query uses >= for the start and < for the end of the time range.
// This means the end time is not included, which prevents two adjacent report
// periods from counting the same job twice.

// Q39: GROUP BY makes the results one row per consumer. QueryRowContext.Scan
// then takes that row from the database and puts the values into the report
// struct.

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
