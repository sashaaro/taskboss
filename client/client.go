// Package taskboss is a Go client for the taskboss PostgreSQL job-queue
// extension, built on github.com/jackc/pgx/v5.
//
// It wraps the SQL surface exposed under the `boss` schema (create_queue,
// send, fetch, complete, fail, ...) and adds a push-based consumer, Work,
// that uses LISTEN/NOTIFY to wake up as soon as a job is enqueued instead of
// polling.
package taskboss

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Client is a handle to a taskboss-enabled database. It is safe for concurrent
// use; queue operations run on a connection pool.
type Client struct {
	pool *pgxpool.Pool
}

// querier is satisfied by both *pgxpool.Pool and *pgxpool.Conn, letting the
// shared operations run either on the pool or on a dedicated worker connection.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Job is a job returned by Fetch / Work.
type Job struct {
	ID         uuid.UUID
	Name       string
	Priority   int32
	Data       json.RawMessage
	State      string
	RetryCount int32
	RetryLimit int32
}

// Queue describes a registered queue and its per-job defaults.
type Queue struct {
	Name             string
	RetryLimit       int32
	RetryDelay       int32
	ExpireSeconds    int32
	RetentionSeconds int32
	CreatedOn        time.Time
}

// QueueOptions overrides the per-job defaults stored on a queue. Nil fields
// fall back to the extension defaults.
type QueueOptions struct {
	RetryLimit       *int
	RetryDelay       *int
	ExpireInSeconds  *int
	RetentionSeconds *int
}

// sendOptions controls how a single job is enqueued. All fields are optional.
type sendOptions struct {
	Priority        *int
	StartAfter      *int // seconds from now
	RetryLimit      *int
	RetryDelay      *int
	ExpireInSeconds *int
}

// SendOption is a functional option for Send.
type SendOption func(*sendOptions)

// WithPriority sets the job priority (higher value = higher priority).
func WithPriority(v int) SendOption { return func(o *sendOptions) { o.Priority = &v } }

// WithStartAfter delays the job by v seconds from now.
func WithStartAfter(v int) SendOption { return func(o *sendOptions) { o.StartAfter = &v } }

// WithRetryLimit sets the maximum number of retries for the job.
func WithRetryLimit(v int) SendOption { return func(o *sendOptions) { o.RetryLimit = &v } }

// WithRetryDelay sets the delay in seconds between retries.
func WithRetryDelay(v int) SendOption { return func(o *sendOptions) { o.RetryDelay = &v } }

// WithExpireInSeconds sets a TTL in seconds after which the job expires.
func WithExpireInSeconds(v int) SendOption { return func(o *sendOptions) { o.ExpireInSeconds = &v } }

// Handler processes a job in Work. Returning nil completes the job; returning
// an error fails it (which retries until the queue's retry limit is reached).
type Handler func(ctx context.Context, job Job) error

// New connects to connString and returns a Client backed by a new pool.
func New(ctx context.Context, connString string) (*Client, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &Client{pool: pool}, nil
}

// NewWithPool wraps an existing pool. The pool is not closed by Close.
func NewWithPool(pool *pgxpool.Pool) *Client {
	return &Client{pool: pool}
}

// Close releases the underlying pool created by New.
func (c *Client) Close() { c.pool.Close() }

// Channel returns the LISTEN/NOTIFY channel taskboss uses for a queue. It
// mirrors the extension's boss.channel(): "boss_<queue>", or "boss_<md5>" when
// the plain name would exceed PostgreSQL's 63-byte channel limit.
func Channel(queue string) string {
	ch := "boss_" + queue
	if len(ch) <= 63 {
		return ch
	}
	sum := md5.Sum([]byte(queue))
	return "boss_" + hex.EncodeToString(sum[:])
}

// CreateQueue creates a queue (idempotent).
func (c *Client) CreateQueue(ctx context.Context, name string, opts *QueueOptions) error {
	o := map[string]any{}
	if opts != nil {
		putInt(o, "retryLimit", opts.RetryLimit)
		putInt(o, "retryDelay", opts.RetryDelay)
		putInt(o, "expireInSeconds", opts.ExpireInSeconds)
		putInt(o, "retentionSeconds", opts.RetentionSeconds)
	}
	_, err := c.pool.Exec(ctx, "SELECT boss.create_queue($1, $2::jsonb)", name, mustJSON(o))
	return err
}

// DeleteQueue deletes a queue and all of its jobs.
func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	_, err := c.pool.Exec(ctx, "SELECT boss.delete_queue($1)", name)
	return err
}

// GetQueues lists all queues.
func (c *Client) GetQueues(ctx context.Context) ([]Queue, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT name, retry_limit, retry_delay, expire_seconds, retention_seconds, created_on
		   FROM boss.get_queues()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queues []Queue
	for rows.Next() {
		var q Queue
		if err := rows.Scan(&q.Name, &q.RetryLimit, &q.RetryDelay,
			&q.ExpireSeconds, &q.RetentionSeconds, &q.CreatedOn); err != nil {
			return nil, err
		}
		queues = append(queues, q)
	}
	return queues, rows.Err()
}

// Send enqueues a job and returns its id. data is JSON-encoded; pass nil for an
// empty payload. A NOTIFY is emitted so Work consumers wake up immediately.
func (c *Client) Send(ctx context.Context, queue string, data any, opts ...SendOption) (uuid.UUID, error) {
	var so sendOptions
	for _, opt := range opts {
		opt(&so)
	}
	o := map[string]any{}
	putInt(o, "priority", so.Priority)
	putInt(o, "startAfter", so.StartAfter)
	putInt(o, "retryLimit", so.RetryLimit)
	putInt(o, "retryDelay", so.RetryDelay)
	putInt(o, "expireInSeconds", so.ExpireInSeconds)
	var idText string
	err := c.pool.QueryRow(ctx,
		"SELECT boss.send($1, $2::jsonb, $3::jsonb)::text",
		queue, encodeData(data), mustJSON(o),
	).Scan(&idText)
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(idText)
}

// Fetch atomically claims up to batchSize ready jobs (FOR UPDATE SKIP LOCKED),
// moving them to the active state.
func (c *Client) Fetch(ctx context.Context, queue string, batchSize int) ([]Job, error) {
	return fetch(ctx, c.pool, queue, batchSize)
}

// Complete marks an active job completed, storing optional JSON output. It
// returns false if the job was not in the active state.
func (c *Client) Complete(ctx context.Context, queue string, id uuid.UUID, output any) (bool, error) {
	return c.boolCall(ctx, "boss.complete", queue, id, output)
}

// Fail fails an active job. If retries remain it is rescheduled (and a NOTIFY
// is emitted); otherwise it moves to the failed state. Returns false if the job
// was not active.
func (c *Client) Fail(ctx context.Context, queue string, id uuid.UUID, output any) (bool, error) {
	return c.boolCall(ctx, "boss.fail", queue, id, output)
}

func (c *Client) boolCall(ctx context.Context, fn, queue string, id uuid.UUID, output any) (bool, error) {
	var ok bool
	err := c.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT %s($1, $2::uuid, $3::jsonb)", fn),
		queue, id.String(), encodeData(output),
	).Scan(&ok)
	return ok, err
}

// Work claims jobs from a queue and runs handler for each, blocking until ctx
// is cancelled. It first drains any backlog, then sleeps on LISTEN/NOTIFY,
// waking the instant a new job is enqueued. The handler's result decides
// whether each job is completed (nil) or failed (error).
func (c *Client) Work(ctx context.Context, queue string, handler Handler) error {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// pgx.Identifier.Sanitize quotes the channel so it matches pg_notify exactly.
	listen := "LISTEN " + pgx.Identifier{Channel(queue)}.Sanitize()
	if _, err := conn.Exec(ctx, listen); err != nil {
		return err
	}

	for {
		// Drain everything currently available before sleeping.
		for {
			jobs, err := fetch(ctx, conn, queue, 1)
			if err != nil {
				return err
			}
			if len(jobs) == 0 {
				break
			}
			if err := c.process(ctx, conn, queue, jobs[0], handler); err != nil {
				return err
			}
		}

		// Wait for the next NOTIFY (or cancellation).
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return ctx.Err()
			}
			return err
		}
	}
}

func (c *Client) process(ctx context.Context, q querier, queue string, job Job, handler Handler) error {
	herr := handler(ctx, job)
	var dbErr error
	if herr != nil {
		_, dbErr = boolCallOn(ctx, q, "boss.fail", queue, job.ID,
			map[string]any{"error": herr.Error()})
	} else {
		_, dbErr = boolCallOn(ctx, q, "boss.complete", queue, job.ID, nil)
	}
	return dbErr
}

func fetch(ctx context.Context, q querier, queue string, batchSize int) ([]Job, error) {
	rows, err := q.Query(ctx,
		`SELECT id::text, name, priority, data, state, retry_count, retry_limit
		   FROM boss.fetch($1, $2)`,
		queue, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var (
			job    Job
			idText string
			data   []byte
		)
		if err := rows.Scan(&idText, &job.Name, &job.Priority, &data,
			&job.State, &job.RetryCount, &job.RetryLimit); err != nil {
			return nil, err
		}
		id, err := uuid.Parse(idText)
		if err != nil {
			return nil, err
		}
		job.ID = id
		job.Data = json.RawMessage(data)
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func boolCallOn(ctx context.Context, q querier, fn, queue string, id uuid.UUID, output any) (bool, error) {
	var ok bool
	err := q.QueryRow(ctx,
		fmt.Sprintf("SELECT %s($1, $2::uuid, $3::jsonb)", fn),
		queue, id.String(), encodeData(output),
	).Scan(&ok)
	return ok, err
}

func encodeData(data any) string {
	if data == nil {
		return "{}"
	}
	if raw, ok := data.(json.RawMessage); ok {
		if len(raw) == 0 {
			return "{}"
		}
		return string(raw)
	}
	return mustJSON(data)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		// Marshaling client-supplied options/data should not fail in practice.
		panic(fmt.Sprintf("taskboss: marshal: %v", err))
	}
	return string(b)
}

func putInt(m map[string]any, key string, v *int) {
	if v != nil {
		m[key] = *v
	}
}

// Ptr is a small helper for building *SendOptions / *QueueOptions inline.
func Ptr[T any](v T) *T { return &v }
