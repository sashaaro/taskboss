//! Database schema and SQL-level queue operations.
//!
//! Everything lives in a dedicated `boss` schema. The DDL, the hot-path
//! functions (`send`/`fetch`) and the maintenance routine are shipped as raw
//! SQL via [`extension_sql!`] so they execute atomically and can use
//! PostgreSQL features (`SKIP LOCKED`, `pg_notify`, partial indexes) directly.
//!
//! The control functions (`create_queue`, `delete_queue`) are implemented in
//! Rust over SPI in [`crate::boss`].

use pgrx::prelude::*;

extension_sql!(
    r#"
CREATE SCHEMA IF NOT EXISTS boss;

-- Lifecycle of a job. `created` -> `active` -> (`completed` | `retry` | `failed`).
CREATE TYPE boss.job_state AS ENUM (
    'created',
    'retry',
    'active',
    'completed',
    'cancelled',
    'failed'
);

-- Queue registry. Holds per-queue defaults inherited by each job.
CREATE TABLE boss.queue (
    name              text PRIMARY KEY,
    retry_limit       integer NOT NULL DEFAULT 2,
    retry_delay       integer NOT NULL DEFAULT 0,
    expire_seconds    integer NOT NULL DEFAULT 900,                 -- 15 minutes
    retention_seconds integer NOT NULL DEFAULT (60 * 60 * 24 * 14), -- 14 days
    created_on        timestamptz NOT NULL DEFAULT now()
);

-- Main job table. Defaults mirror pg-boss; only `name` is required to insert.
CREATE TABLE boss.job (
    id             uuid NOT NULL DEFAULT gen_random_uuid(),
    name           text NOT NULL REFERENCES boss.queue(name) ON DELETE CASCADE,
    priority       integer NOT NULL DEFAULT 0,
    data           jsonb,
    state          boss.job_state NOT NULL DEFAULT 'created',
    retry_limit    integer NOT NULL DEFAULT 2,
    retry_count    integer NOT NULL DEFAULT 0,
    retry_delay    integer NOT NULL DEFAULT 0,
    expire_seconds integer NOT NULL DEFAULT 900,
    start_after    timestamptz NOT NULL DEFAULT now(),
    created_on     timestamptz NOT NULL DEFAULT now(),
    started_on     timestamptz,
    completed_on   timestamptz,
    keep_until     timestamptz NOT NULL DEFAULT now() + interval '14 days',
    output         jsonb,
    CONSTRAINT job_pkey PRIMARY KEY (id)
);

-- Hot path for fetch(): only the queueable rows are indexed.
CREATE INDEX job_fetch_idx
    ON boss.job (name, priority DESC, created_on, id)
    WHERE state IN ('created', 'retry');

-- Deterministic LISTEN/NOTIFY channel for a queue. NOTIFY channel names are
-- limited to 63 bytes, so long queue names fall back to a hashed channel.
CREATE FUNCTION boss.channel(queue_name text)
RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
    SELECT CASE
        WHEN length('boss_' || queue_name) <= 63 THEN 'boss_' || queue_name
        ELSE 'boss_' || md5(queue_name)
    END;
$$;

-- Enqueue a job and wake up any listening consumer. Returns the new job id.
-- `options` (jsonb) may contain: priority, startAfter (seconds or ISO ts),
-- retryLimit, retryDelay, expireInSeconds.
CREATE FUNCTION boss.send(queue_name text, data jsonb DEFAULT '{}', options jsonb DEFAULT '{}')
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    q             boss.queue%ROWTYPE;
    v_id          uuid;
    v_start_after timestamptz;
BEGIN
    SELECT * INTO q FROM boss.queue WHERE name = queue_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'queue "%" does not exist', queue_name USING ERRCODE = 'undefined_object';
    END IF;

    v_start_after := CASE
        WHEN options->>'startAfter' IS NULL          THEN now()
        WHEN options->>'startAfter' ~ '^[0-9]+$'     THEN now() + ((options->>'startAfter')::int * interval '1 second')
        ELSE (options->>'startAfter')::timestamptz
    END;

    INSERT INTO boss.job
        (name, priority, data, retry_limit, retry_delay, expire_seconds, start_after, keep_until)
    VALUES (
        queue_name,
        COALESCE((options->>'priority')::int, 0),
        COALESCE(data, '{}'::jsonb),
        COALESCE((options->>'retryLimit')::int, q.retry_limit),
        COALESCE((options->>'retryDelay')::int, q.retry_delay),
        COALESCE((options->>'expireInSeconds')::int, q.expire_seconds),
        v_start_after,
        now() + (q.retention_seconds * interval '1 second')
    )
    RETURNING id INTO v_id;

    PERFORM pg_notify(boss.channel(queue_name), v_id::text);
    RETURN v_id;
END;
$$;

-- Atomically claim up to `batch_size` ready jobs, moving them to `active`.
-- SKIP LOCKED gives exactly-once delivery across competing consumers.
CREATE FUNCTION boss.fetch(queue_name text, batch_size integer DEFAULT 1)
RETURNS SETOF boss.job
LANGUAGE sql
AS $$
    WITH next AS (
        SELECT id
        FROM boss.job
        WHERE name = queue_name
          AND state IN ('created', 'retry')
          AND start_after <= now()
        ORDER BY priority DESC, created_on, id
        LIMIT batch_size
        FOR UPDATE SKIP LOCKED
    )
    UPDATE boss.job j
       SET state = 'active', started_on = now()
      FROM next
     WHERE j.id = next.id
    RETURNING j.*;
$$;

-- Mark an active job completed, storing optional output. Returns false if the
-- job was not active (already finished, wrong queue, unknown id).
CREATE FUNCTION boss.complete(queue_name text, job_id uuid, output jsonb DEFAULT '{}')
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    n integer;
BEGIN
    UPDATE boss.job
       SET state = 'completed', completed_on = now(), output = $3
     WHERE name = $1 AND id = $2 AND state = 'active';
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n > 0;
END;
$$;

-- Fail an active job: retry if attempts remain (scheduling after retry_delay
-- and notifying listeners), otherwise move to `failed`.
CREATE FUNCTION boss.fail(queue_name text, job_id uuid, output jsonb DEFAULT '{}')
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    j boss.job%ROWTYPE;
BEGIN
    SELECT * INTO j FROM boss.job
     WHERE name = $1 AND id = $2 AND state = 'active'
     FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    IF j.retry_count < j.retry_limit THEN
        UPDATE boss.job
           SET state = 'retry',
               retry_count = retry_count + 1,
               start_after = now() + (retry_delay * interval '1 second'),
               started_on = NULL,
               output = $3
         WHERE id = j.id;
        PERFORM pg_notify(boss.channel($1), j.id::text);
    ELSE
        UPDATE boss.job
           SET state = 'failed', completed_on = now(), started_on = NULL, output = $3
         WHERE id = j.id;
    END IF;
    RETURN true;
END;
$$;

-- List all queues.
CREATE FUNCTION boss.get_queues()
RETURNS SETOF boss.queue
LANGUAGE sql STABLE
AS $$
    SELECT * FROM boss.queue ORDER BY name;
$$;

-- Background maintenance: expire stale active jobs and purge old terminal jobs.
-- Invoked periodically by the background worker.
CREATE FUNCTION boss.maintain()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Active jobs that overran expire_seconds are retried or failed.
    UPDATE boss.job
       SET state = CASE WHEN retry_count < retry_limit THEN 'retry'::boss.job_state
                        ELSE 'failed'::boss.job_state END,
           retry_count = CASE WHEN retry_count < retry_limit THEN retry_count + 1
                              ELSE retry_count END,
           start_after = CASE WHEN retry_count < retry_limit THEN now() + (retry_delay * interval '1 second')
                              ELSE start_after END,
           completed_on = CASE WHEN retry_count < retry_limit THEN NULL ELSE now() END,
           started_on = NULL,
           output = COALESCE(output, '{}'::jsonb) || '{"__expired": true}'::jsonb
     WHERE state = 'active'
       AND started_on IS NOT NULL
       AND now() - started_on > (expire_seconds * interval '1 second');

    -- Terminal jobs past their retention window are removed.
    DELETE FROM boss.job
     WHERE state IN ('completed', 'failed', 'cancelled')
       AND keep_until < now();
END;
$$;
"#,
    name = "boss_bootstrap",
);
