# my_extension

Нативное расширение очереди задач для PostgreSQL, написанное на Rust с использованием [pgrx](https://github.com/pgcentralfoundation/pgrx). Вдохновлено [pg-boss](https://github.com/timgit/pg-boss).

В отличие от pg-boss (Node.js-библиотека), это расширение живёт прямо внутри PostgreSQL — без внешних процессов и дополнительных зависимостей.

## Возможности (planned)

- Надёжная доставка задач через `SKIP LOCKED` — гарантия "exactly-once" обработки
- Приоритеты очередей и dead letter queue
- Отложенный запуск и cron-расписание
- Автоматические retry с экспоненциальным backoff
- Rate limiting и debounce через политики очередей
- Fan-out через pub/sub API
- Работа в распределённых средах (multi-master / Kubernetes)

## Требования

- PostgreSQL 18
- Rust toolchain + `cargo pgrx`

## Быстрый старт

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
CREATE EXTENSION my_extension;
SELECT hello_my_extension();
```

## Разработка

```bash
# Сборка
cargo pgrx build

# Тесты (поднимает временный инстанс PostgreSQL)
cargo pgrx test pg18

# Бенчмарки
cargo pgrx bench pg18
```

## Вдохновение

[pg-boss](https://github.com/timgit/pg-boss) — отличная реализация очереди на PostgreSQL для Node.js. Этот проект преследует ту же цель, но реализует логику очереди как нативное серверное расширение PostgreSQL, минуя накладные расходы внешнего процесса.
