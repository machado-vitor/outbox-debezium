import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

/// Each test writes rows the way the application would (orders + outbox in one
/// statement) and reads the topic back. Infrastructure lives in Env.
class DebeziumOutboxTest extends Env {

    @Test
    void publishesTheOrderEventWithKeyHeadersAndPayload() throws Exception {
        var orderId = insertOrder(db, "cust-1");

        var records = consume(1);
        assertEquals(1, records.size());
        var record = records.getFirst();
        assertEquals(orderId, record.key(), "key is the aggregate id");
        assertEquals("order.created", header(record, "event_type"));
        assertEquals(scalar("SELECT event_id::text FROM outbox"), header(record, "event_id"));
        assertTrue(record.value().contains("\"customer_id\": \"cust-1\""), "value is the payload as written");
        assertNull(scalar("SELECT published_at FROM outbox"), "Debezium never touches the table; the WAL position is the mark");
    }

    /// The dual-write problem, solved: the outbox row lives in the business transaction,
    /// so a rollback takes the event with it. pgoutput streams commits, not writes.
    @Test
    void aRolledBackTransactionPublishesNothing() throws Exception {
        try (var tx = connect()) {
            tx.setAutoCommit(false);
            insertOrder(tx, "rolled-back");
            tx.rollback();
        }
        var committed = insertOrder(db, "committed");

        assertEquals(List.of(committed), keys(consume(1)), "only the committed order reaches the topic");
        assertEquals("1", scalar("SELECT count(*) FROM outbox"));
    }

    /// Tx A takes the lower id but commits after tx B. The WAL is in commit order, so Debezium sees B, then A.
    @Test
    void aRowThatBecomesVisibleLateIsStillPublished() throws Exception {
        try (var a = connect()) {
            a.setAutoCommit(false);
            var idA = insertOrder(a, "A");   // lower id, not committed: not in the WAL yet
            var idB = insertOrder(db, "B");  // higher id, committed

            assertEquals(List.of(idB), keys(consume(1)));

            a.commit();                      // A's commit record lands in the WAL after B's

            assertEquals(List.of(idB, idA), keys(consume(2)), "topic is in commit order");
            assertEquals(idA, scalar("SELECT aggregate_id FROM outbox ORDER BY id LIMIT 1"), "table is in id order");
        }
    }

    /// key = aggregate_id, so every event of one order lands on one partition, in order.
    @Test
    void eventsOfOneAggregateShareAPartitionInOrder() throws Exception {
        var orderId = insertOrder(db, "cust-1");
        updateOrder(db, orderId);

        var records = consume(2);
        assertEquals(List.of(orderId, orderId), keys(records));
        assertEquals(1, partitions(records).size(), "one aggregate, one partition");
        assertEquals(List.of("order.created", "order.updated"),
                records.stream().map(r -> header(r, "event_type")).toList());
    }

    /// At-least-once: the replication slot keeps the WAL while the connector is gone,
    /// so rows written in the gap are published when it comes back. Nothing is lost.
    @Test
    void rowsWrittenWhileTheConnectorIsDownArePublishedWhenItReturns() throws Exception {
        deleteConnector();
        String orderId;
        try {
            orderId = insertOrder(db, "cust-1");
            assertEquals(List.of(), consume(0), "nothing is published while the connector is down");
            assertEquals("1", scalar("SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'outbox_slot'"),
                    "the slot outlives the connector and pins the WAL");
        } finally {
            registerConnector();
        }
        assertEquals(List.of(orderId), keys(consume(1)));
    }
}
