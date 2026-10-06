# outbox-debezium

Transactional outbox drained by Debezium. `orders` row and `outbox` row are written in one
statement; Debezium reads the Postgres WAL (`wal_level=logical`, `pgoutput`) and the
`EventRouter` SMT turns each `outbox` INSERT into a record: key = `aggregate_id`,
value = `payload`, headers `event_id` / `event_type`. No code, one JSON (`connector.json`).

```sh
mvn test
```

That is the whole demo. `DebeziumOutboxTest` (infrastructure in `Env`) starts a real Postgres (logical decoding on),
a real Kafka and a real Debezium Connect (Testcontainers), registers `connector.json`,
writes rows the way an application would, and prints what reached the topic:

| test | shows |
|---|---|
| `publishesTheOrderEventWithKeyHeadersAndPayload` | key = `aggregate_id`, headers `event_id` / `event_type`, value = payload. `published_at` stays NULL: Debezium never touches the table, the WAL position is the mark |
| `aRolledBackTransactionPublishesNothing` | the outbox row lives in the business transaction, so a rollback takes the event with it. pgoutput streams commits, not writes |
| `aRowThatBecomesVisibleLateIsStillPublished` | tx A takes the lower id but commits after tx B. The table is in id order, the topic is in commit order. Nothing is skipped |
| `eventsOfOneAggregateShareAPartitionInOrder` | key = `aggregate_id`, so every event of one order lands on one partition, in order |
| `rowsWrittenWhileTheConnectorIsDownArePublishedWhenItReturns` | the replication slot outlives the connector and pins the WAL; rows written in the gap are published on return |

Notes: `snapshot.mode=no_data` means rows that existed before the connector was registered
are not published. Deleting the connector does not drop its replication slot; the WAL
grows until you do (`SELECT pg_drop_replication_slot('outbox_slot')`).

Siblings: [outbox-relay](https://github.com/machado-vitor/outbox-relay) (own poller, same table) and
[outbox-jdbc-source](https://github.com/machado-vitor/outbox-jdbc-source) (the Kafka Connect poller that loses events).
