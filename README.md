# outbox-debezium

Transactional outbox drained by Debezium. `orders` row and `outbox` row are written in one
statement; Debezium reads the Postgres WAL (`wal_level=logical`, `pgoutput`) and the
`EventRouter` SMT turns each `outbox` INSERT into a record: key = `aggregate_id`,
value = `payload`, headers `event_id` / `event_type`. No code, one JSON (`connector.json`).

```sh
mvn test
```

That is the whole demo. `DebeziumOutboxTest` starts a real Postgres (logical decoding on),
a real Kafka and a real Debezium Connect (Testcontainers), registers `connector.json`,
writes rows the way an application would, and prints what reached the topic:

| test | shows |
|---|---|
| `publishesTheOrderEventWithKeyHeadersAndPayload` | key = `aggregate_id`, headers `event_id` / `event_type`, value = payload. `published_at` stays NULL: Debezium never touches the table, the WAL position is the mark |
| `aRowThatBecomesVisibleLateIsStillPublished` | tx A takes the lower id but commits after tx B. The WAL is in commit order: B is published, then A once it commits. Nothing is skipped |

Notes: `snapshot.mode=no_data` means rows that existed before the connector was registered
are not published. Deleting the connector does not drop its replication slot; the WAL
grows until you do (`SELECT pg_drop_replication_slot('outbox_slot')`).

Siblings: [outbox-relay](../outbox-relay) (own poller, same table) and
[outbox-jdbc-source](../outbox-jdbc-source) (the Kafka Connect poller that loses events).
