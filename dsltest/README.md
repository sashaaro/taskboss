# dsltest — декларативный DSL для интеграционных тестов taskboss

`dsltest` парсит сценарии (`*.scenario`) с помощью [`winnow`](https://github.com/winnow-rs/winnow)
и выполняет их против **запущенного** PostgreSQL с установленным расширением `taskboss`.
Каждый клиент `#N` — это отдельная сессия (TCP-соединение), поэтому проверяются и
конкуренция консьюмеров (`FOR UPDATE SKIP LOCKED`), и пробуждение по `LISTEN/NOTIFY`
между разными сессиями.

## Запуск

```bash
# 1. поднять управляемый pgrx-инстанс с расширением (порт 28818, БД taskboss)
cargo pgrx run pg18        # \q сразу — инстанс остаётся запущенным

# 2. прогнать сценарии (лежат в корне репозитория, в scenarios/)
cargo run -p dsltest -- scenarios                  # директория целиком
cargo run -p dsltest -- scenarios/basic_delivery.scenario

# DSN можно переопределить
TASKBOSS_DSN=postgres://user@host:5432/db cargo run -p dsltest
```

Коды возврата: `0` — все прошли, `1` — упал `assert`/`check` или запрос к БД,
`2` — ошибка парсинга, `3` — нет соединения/файлов.

## Грамматика

Один оператор на строку. Перед оператором можно указать клиента `#N` (по умолчанию `#1`).
Строка, начинающаяся с `#` и **не цифры** (`# ...`), — комментарий; `#1` — это клиент.

```
scenario <имя>:

[#N] create queue <имя> [retryLimit=I] [retryDelay=I] [expireInSeconds=I] [retentionSeconds=I]
[#N] delete queue <имя>
[#N] maintain

[#N] push <очередь> [message "<строка>" | data <json>]
                    [priority=I] [startAfter=I] [retryLimit=I] [retryDelay=I] [expireInSeconds=I]

[#N] consume <очередь> -> <var> [within <dur>]     # переднеплановый claim, связывает job с var
[#N] spawn consume <очередь> -> <var> [within <dur>]   # фоновый консьюмер (отдельный поток)
     await <var>                                   # дождаться фонового consume

[#N] ack  <var> [output <json>]                    # complete
[#N] fail <var> [output <json>]

[#N] assert queue <очередь> empty
[#N] assert queue <очередь> size <I>
     check  <var> state <created|retry|active|completed|cancelled|failed>
     check  <var> ack [within <dur>]               # дождаться состояния completed
     check  <var> empty                            # фоновый consume истёк без задачи
     assert <var> == <var>                          # один и тот же job id
     assert exactly_one_claimed <var> <var> [...]   # ровно один из var получил задачу
```

`<dur>` — `<число>(ms|s|m)`, например `500ms`, `1s`, `2m`.

Допустимы только реальные опции расширения. Неизвестный ключ (например `capacity`) или
голый флаг (`durable`) — это ошибка парсинга с указанием строки.

## Сценарии

Файлы сценариев лежат в корне репозитория, в каталоге [`scenarios/`](../scenarios):

- [basic_delivery](../scenarios/basic_delivery.scenario) — один клиент: enqueue → claim → ack → очередь пуста.
- [notify_wakeup](../scenarios/notify_wakeup.scenario) — `#2` ждёт на `LISTEN/NOTIFY`, `#1 push` будит его.
- [competing_consumers](../scenarios/competing_consumers.scenario) — два консьюмера за одну задачу, ровно один выигрывает.
- [retry_then_succeed](../scenarios/retry_then_succeed.scenario) — `fail` → `retry` → повторный claim того же job → `ack`.

## Структура

- `src/parser.rs` — winnow-парсер (`текст -> Scenario`).
- `src/ast.rs` — AST и типизированные опции.
- `src/runner.rs` — исполнитель: сессии по `#N`, фоновые consume, ассерты.
- `src/error.rs` — ошибки с номером строки и кодом возврата.
