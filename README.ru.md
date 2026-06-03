# taskboss

Нативное расширение очереди задач для PostgreSQL, написанное на Rust с использованием [pgrx](https://github.com/pgcentralfoundation/pgrx). Вдохновлено [pg-boss](https://github.com/timgit/pg-boss).

В отличие от pg-boss (Node.js-библиотека), это расширение живёт прямо внутри PostgreSQL — без внешних процессов и дополнительных зависимостей.

## Возможности (v1 / MVP)

- Реестр очередей: `boss.create_queue` / `boss.delete_queue` / `boss.get_queues`
- Надёжная доставка задач через `SKIP LOCKED` — exactly-once-захват конкурентными консьюмерами
- Push-доставка новых задач через встроенный `LISTEN`/`NOTIFY`
- Приоритеты, отложенный запуск (`startAfter`), базовый retry с задержкой
- Фоновый воркер: автоматический expire зависших задач и удаление по retention

Отложено на будущие версии: cron-расписания, pub/sub, политики очередей (singleton/short/stately),
партиционирование, heartbeat-мониторинг, throttle/debounce, dead-letter.

## Требования

- PostgreSQL 18
- Rust toolchain + `cargo pgrx`
- Для фонового воркера обслуживания: `shared_preload_libraries = 'taskboss'` в `postgresql.conf`
  (требует рестарта PostgreSQL) и GUC `taskboss.database` с именем БД, где установлено расширение.

## Быстрый старт

### Docker

```bash
docker run -d --name taskboss \
  -e POSTGRES_PASSWORD=secret \
  -p 5432:5432 \
  ghcr.io/sashaaro/taskboss:latest
```

Подключиться к запущенному контейнеру:

```bash
docker exec -it taskboss psql -U postgres
```

### Из исходников

```bash
# Установить cargo-pgrx
cargo install cargo-pgrx

# Инициализировать управляемые инсталляции PostgreSQL
cargo pgrx init

# Запустить расширение в PostgreSQL 18
cargo pgrx run pg18
```

После подключения к psql:

```sql
CREATE EXTENSION taskboss;

-- создать очередь и отправить задачу
SELECT boss.create_queue('email-welcome');
SELECT boss.send('email-welcome', '{"to": "a@b.c"}');

-- consumer: атомарно забрать и завершить задачу
SELECT * FROM boss.fetch('email-welcome', 1);
SELECT boss.complete('email-welcome', '<job-id>', '{"ok": true}');
```

### Push-доставка через LISTEN/NOTIFY

Чтобы не опрашивать очередь в цикле, консьюмер подписывается на канал очереди и просыпается
по уведомлению, после чего атомарно забирает задачу через `fetch`:

```sql
LISTEN boss_email_welcome;                       -- канал = boss_<имя_очереди>
-- ... клиент блокируется до NOTIFY от boss.send() ...
SELECT * FROM boss.fetch('email-welcome', 1);
```

## Параметры функций

- `boss.send(name, data jsonb, options jsonb)` — `options`: `priority`, `startAfter`
  (секунды или ISO-строка), `retryLimit`, `retryDelay`, `expireInSeconds`.
- `boss.create_queue(name, options jsonb)` — `options`: `retryLimit`, `retryDelay`,
  `expireInSeconds`, `retentionSeconds` (значения по умолчанию для задач очереди).
- `boss.fetch(name, batch_size)` → `SETOF boss.job`.
- `boss.complete(name, id, output jsonb)` / `boss.fail(name, id, output jsonb)` → `boolean`.

## Разработка

```bash
# Сборка
cargo pgrx build

# Тесты (поднимает временный инстанс PostgreSQL)
cargo pgrx test pg18

# Бенчмарки
cargo pgrx bench pg18
```

## Тесты-сценарии (DSL)

Помимо `pg_test`-тестов, в репозитории есть декларативные интеграционные тесты на
небольшом DSL. Сценарии лежат в каталоге [`scenarios/`](scenarios), а раннер
[`dsltest`](dsltest) (парсер на [winnow](https://github.com/winnow-rs/winnow))
выполняет их против **запущенного** инстанса. Каждый клиент `#N` — это отдельная
сессия, поэтому проверяются и конкуренция консьюмеров (`SKIP LOCKED`), и пробуждение
по `LISTEN`/`NOTIFY` между разными сессиями.

```bash
# 1. поднять инстанс с расширением (порт 28818, БД taskboss); \q — инстанс остаётся жив
cargo pgrx run pg18

# 2. прогнать все сценарии (или указать конкретные файлы)
cargo run -p dsltest -- scenarios
cargo run -p dsltest -- scenarios/basic_delivery.scenario

# DSN можно переопределить через TASKBOSS_DSN
```

Покрытие: доставка и push (`basic_delivery`, `notify_wakeup`); корректность очереди
(`priority_ordering`, `fifo_ordering`, `delayed_start`, `retry_then_succeed`,
`retry_exhaustion`, `retry_delay`, `expire_via_maintain`, `retention_purge`); конкуренция
(`competing_consumers`, `multi_consumer_exactly_once`, `concurrent_producers`).

Полное описание грамматики DSL и список сценариев — в [dsltest/README.md](dsltest/README.md).

## Вдохновение

[pg-boss](https://github.com/timgit/pg-boss) — отличная реализация очереди на PostgreSQL для Node.js. Этот проект преследует ту же цель, но реализует логику очереди как нативное серверное расширение PostgreSQL, минуя накладные расходы внешнего процесса.
