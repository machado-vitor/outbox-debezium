# outbox-debezium

Transactional outbox drained by Debezium. `orders` row and `outbox` row are written in one
statement; Debezium reads the Postgres WAL (`wal_level=logical`, `pgoutput`) and the
`EventRouter` SMT turns each `outbox` INSERT into a record: key = `aggregate_id`,
value = `payload`, headers `event_id` / `event_type`. No code, one JSON.

```sh
make up            # postgres (logical decoding) + kafka + connect, schema, topic
make connector     # register connector.json
make connector-status
make order N=5     # 5 orders, each with its outbox row
make consume       # key = aggregate_id, headers = event_id/event_type, value = payload
make race          # a slow tx with a lower id commits after a fast one: both still arrive
```

Nothing ever sets `published_at`: the WAL position in `connect_offsets` is the "already
read" mark. `snapshot.mode=no_data` means rows that existed before the connector was
registered are not published. Delete the connector and its slot stops being consumed;
the WAL grows until you drop it (`SELECT pg_drop_replication_slot('outbox_slot')`).

Siblings: [outbox-relay](../outbox-relay) (own poller, same table) and
[outbox-jdbc-source](../outbox-jdbc-source) (the Kafka Connect poller that loses events).
