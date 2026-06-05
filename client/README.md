# taskboss Go client

Go-клиент для расширения [`taskboss`](../README.md) на базе
[jackc/pgx/v5](https://github.com/jackc/pgx). Оборачивает функции схемы `boss`
(`create_queue`, `send`, `fetch`, `complete`, `fail`, …) и добавляет
push-консьюмер `Work`, который через `LISTEN`/`NOTIFY` просыпается сразу при
появлении новой задачи, а не опрашивает БД в цикле.

## Пример

```go
ctx := context.Background()
c, _ := taskboss.New(ctx, "postgres://postgres:secret@localhost:5432/postgres")
defer c.Close()

_ = c.CreateQueue(ctx, "email", nil)

// producer
id, _ := c.Send(ctx, "email", map[string]any{"to": "a@b.c"},
    &taskboss.SendOptions{Priority: taskboss.Ptr(10)})

// consumer: блокируется до отмены ctx, обрабатывая задачи по мере поступления
_ = c.Work(ctx, "email", func(ctx context.Context, job taskboss.Job) error {
    return sendEmail(job.Data) // nil -> complete, error -> fail (с retry)
})
```

Альтернатива `Work` — ручной pull-цикл: `Fetch` (атомарный захват через
`SKIP LOCKED`) + `Complete`/`Fail`.

## Тесты

Тестам нужен запущенный PostgreSQL с установленным `taskboss`. DSN берётся из
`TASKBOSS_DSN` (по умолчанию — инстанс `cargo pgrx run pg18`):

```bash
TASKBOSS_DSN=postgres://postgres:secret@localhost:5432/postgres go test ./...
```

Каждый тест создаёт уникальную очередь и удаляет её за собой; если БД
недоступна — тест падает (`t.Fatal`).
