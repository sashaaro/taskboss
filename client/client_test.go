package taskboss

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// These tests need a running PostgreSQL with the taskboss extension available.
// Point TASKBOSS_DSN at it; it defaults to the instance started by
// `cargo pgrx run pg18`.
//
//	TASKBOSS_DSN=postgres://user@localhost:28818/taskboss go test ./client/...
func dsn() string {
	if v := os.Getenv("TASKBOSS_DSN"); v != "" {
		return v
	}
	return "postgres://sasha@localhost:28818/taskboss"
}

// newTestClient connects, ensures the extension exists, and creates a uniquely
// named queue that is dropped when the test finishes. It fails the test if the
// database is unreachable.
func newTestClient(t *testing.T) (*Client, string) {
	t.Helper()
	ctx := context.Background()

	c, err := New(ctx, dsn())
	if err != nil {
		t.Fatalf("cannot connect to %s: %v", dsn(), err)
	}

	if _, err := c.pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS taskboss"); err != nil {
		c.Close()
		t.Fatalf("taskboss extension unavailable: %v", err)
	}

	queue := "test_" + uuid.NewString()[:8]
	if err := c.CreateQueue(ctx, queue, nil); err != nil {
		c.Close()
		t.Fatalf("create queue: %v", err)
	}

	t.Cleanup(func() {
		_ = c.DeleteQueue(context.Background(), queue)
		c.Close()
	})
	return c, queue
}

func TestCreateAndGetQueues(t *testing.T) {
	c, queue := newTestClient(t)
	ctx := context.Background()

	queues, err := c.GetQueues(ctx)
	if err != nil {
		t.Fatalf("get queues: %v", err)
	}
	found := false
	for _, q := range queues {
		if q.Name == queue {
			found = true
			if q.RetryLimit != 2 || q.ExpireSeconds != 900 {
				t.Errorf("unexpected defaults: %+v", q)
			}
		}
	}
	if !found {
		t.Fatalf("queue %q not returned by GetQueues", queue)
	}
}

func TestSendFetchComplete(t *testing.T) {
	c, queue := newTestClient(t)
	ctx := context.Background()

	id, err := c.Send(ctx, queue, map[string]any{"to": "a@b.c"}, nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if id == uuid.Nil {
		t.Fatal("send returned nil id")
	}

	jobs, err := c.Fetch(ctx, queue, 10)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].ID != id {
		t.Errorf("fetched id %v, want %v", jobs[0].ID, id)
	}
	if jobs[0].State != "active" {
		t.Errorf("state = %q, want active", jobs[0].State)
	}

	// The only job is now active, so a second fetch finds nothing.
	again, err := c.Fetch(ctx, queue, 10)
	if err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("expected no jobs on second fetch, got %d", len(again))
	}

	ok, err := c.Complete(ctx, queue, id, map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !ok {
		t.Error("complete returned false")
	}
}

func TestFailRetriesThenFails(t *testing.T) {
	c, queue := newTestClient(t)
	ctx := context.Background()

	// retryLimit=1: first failure retries, second exhausts to failed.
	id, err := c.Send(ctx, queue, nil, &SendOptions{RetryLimit: Ptr(1)})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	mustFetchOne(t, c, queue) // claim attempt 1
	if ok, err := c.Fail(ctx, queue, id, map[string]any{"err": "boom"}); err != nil || !ok {
		t.Fatalf("fail #1: ok=%v err=%v", ok, err)
	}
	if got := jobState(t, c, queue, id); got != "retry" {
		t.Fatalf("after fail #1 state = %q, want retry", got)
	}

	mustFetchOne(t, c, queue) // claim attempt 2
	if ok, err := c.Fail(ctx, queue, id, nil); err != nil || !ok {
		t.Fatalf("fail #2: ok=%v err=%v", ok, err)
	}
	if got := jobState(t, c, queue, id); got != "failed" {
		t.Fatalf("after fail #2 state = %q, want failed", got)
	}
}

func TestPriorityOrdering(t *testing.T) {
	c, queue := newTestClient(t)
	ctx := context.Background()

	if _, err := c.Send(ctx, queue, nil, &SendOptions{Priority: Ptr(1)}); err != nil {
		t.Fatalf("send low: %v", err)
	}
	hi, err := c.Send(ctx, queue, nil, &SendOptions{Priority: Ptr(10)})
	if err != nil {
		t.Fatalf("send high: %v", err)
	}

	jobs := mustFetchOne(t, c, queue)
	if jobs.ID != hi {
		t.Errorf("fetched %v, want highest-priority %v", jobs.ID, hi)
	}
}

func TestWorkConsumesViaNotify(t *testing.T) {
	c, queue := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processed := make(chan Job, 1)
	var workErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c.Work(ctx, queue, func(_ context.Context, job Job) error {
			processed <- job
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			workErr = err
		}
	}()

	// Give Work a moment to issue LISTEN before we publish.
	time.Sleep(200 * time.Millisecond)

	id, err := c.Send(ctx, queue, map[string]any{"hello": "world"}, nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case job := <-processed:
		if job.ID != id {
			t.Errorf("processed %v, want %v", job.ID, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process the job within 5s")
	}

	// The handler returned nil; the worker completes the job right after,
	// asynchronously. Wait for that to land before cancelling.
	state := "active"
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if state = jobState(t, c, queue, id); state == "completed" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	wg.Wait()
	if workErr != nil {
		t.Errorf("Work returned: %v", workErr)
	}
	if state != "completed" {
		t.Errorf("state = %q, want completed", state)
	}
}

func mustFetchOne(t *testing.T, c *Client, queue string) Job {
	t.Helper()
	jobs, err := c.Fetch(context.Background(), queue, 1)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	return jobs[0]
}

func jobState(t *testing.T, c *Client, queue string, id uuid.UUID) string {
	t.Helper()
	var state string
	err := c.pool.QueryRow(context.Background(),
		"SELECT state::text FROM boss.job WHERE name = $1 AND id = $2",
		queue, id.String(),
	).Scan(&state)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	return state
}
