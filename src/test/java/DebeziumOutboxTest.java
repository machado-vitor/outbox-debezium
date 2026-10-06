import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/// Real Postgres (logical decoding on), real Kafka, real Debezium Connect with
/// connector.json registered. Each test writes rows the way the application
/// would (orders + outbox in one statement) and reads the topic back.
class DebeziumOutboxTest {

    static final String TOPIC = "order.events";
    static final Network NET = Network.newNetwork();

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6-alpine")
            .withNetwork(NET).withNetworkAliases("postgres")
            .withUsername("outbox").withPassword("outbox").withDatabaseName("outbox")
            .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=4", "-c", "max_replication_slots=4")
            .withCopyFileToContainer(MountableFile.forHostPath("schema.sql"), "/docker-entrypoint-initdb.d/schema.sql");

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1")
            .withNetwork(NET).withNetworkAliases("kafka").withListener("kafka:19092")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    static final GenericContainer<?> CONNECT = new GenericContainer<>("quay.io/debezium/connect:3.3.2.Final")
            .withNetwork(NET).dependsOn(POSTGRES, KAFKA)
            .withEnv(Map.of(
                    "BOOTSTRAP_SERVERS", "kafka:19092",
                    "GROUP_ID", "connect",
                    "CONFIG_STORAGE_TOPIC", "connect_configs",
                    "OFFSET_STORAGE_TOPIC", "connect_offsets",
                    "STATUS_STORAGE_TOPIC", "connect_statuses",
                    "CONFIG_STORAGE_REPLICATION_FACTOR", "1",
                    "OFFSET_STORAGE_REPLICATION_FACTOR", "1",
                    "STATUS_STORAGE_REPLICATION_FACTOR", "1"))
            .withExposedPorts(8083)
            .waitingFor(Wait.forHttp("/connectors").forPort(8083).withStartupTimeout(Duration.ofMinutes(2)));

    static Connection db;

    @BeforeAll
    static void start() throws Exception {
        Startables.deepStart(CONNECT).join();
        try (var admin = Admin.create(Map.of("bootstrap.servers", (Object) KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
        }
        db = connect();
        registerConnector();
    }

    @AfterAll
    static void stop() {
        CONNECT.stop();
        KAFKA.stop();
        POSTGRES.stop();
    }

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

    /// Tx A takes the lower id but commits after tx B. The WAL is in commit order, so Debezium sees B, then A.
    @Test
    void aRowThatBecomesVisibleLateIsStillPublished() throws Exception {
        try (var a = connect()) {
            a.setAutoCommit(false);
            insertOutbox(a, "A");   // lower id, not committed: not in the WAL yet
            insertOutbox(db, "B");  // higher id, committed

            assertEquals(Set.of("B"), keys(consume(1)));

            a.commit();             // A's commit record lands in the WAL after B's

            var records = consume(2);
            assertEquals(2, records.size());
            assertEquals(List.of("B", "A"), records.stream().map(ConsumerRecord::key).toList(), "commit order");
        }
    }

    // --- kafka connect ------------------------------------------------------------

    static void registerConnector() throws Exception {
        var base = "http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083) + "/connectors/debezium-connector";
        var http = HttpClient.newHttpClient();
        var put = HttpRequest.newBuilder(URI.create(base + "/config"))
                .header("content-type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(Files.readString(Path.of("connector.json")))).build();
        var res = http.send(put, HttpResponse.BodyHandlers.ofString());
        assertTrue(res.statusCode() / 100 == 2, "register: " + res.body());

        var status = HttpRequest.newBuilder(URI.create(base + "/status")).GET().build();
        var deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        String body;
        do {
            Thread.sleep(500);
            body = http.send(status, HttpResponse.BodyHandlers.ofString()).body();
        } while (!body.contains("\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"") && System.nanoTime() < deadline);
        assertTrue(body.contains("\"state\":\"RUNNING\""), "task never started: " + body);
        System.out.println("  connect <- " + body);
    }

    // --- writing the way the application does --------------------------------------

    /// The orders row and its outbox row in ONE statement: atomic by construction.
    static String insertOrder(Connection c, String customer) throws Exception {
        var rs = c.createStatement().executeQuery("""
                WITH o AS (INSERT INTO orders (id, customer_id, amount_cents)
                           VALUES (gen_random_uuid(), '%s', 1000) RETURNING *)
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                SELECT gen_random_uuid(), o.id, 'order.created', to_jsonb(o) FROM o
                RETURNING aggregate_id""".formatted(customer));
        rs.next();
        return rs.getString(1);
    }

    static void insertOutbox(Connection c, String key) throws Exception {
        c.createStatement().execute("""
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                VALUES (gen_random_uuid(), '%s', 'order.created', '{}')""".formatted(key));
    }

    // --- reading both sides ----------------------------------------------------------

    static KafkaConsumer<String, String> topic;
    static final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    /// Empty tables, and a consumer parked at the end of the topic so each test
    /// reads back only what it produced.
    @BeforeEach
    void startClean() throws Exception {
        db.createStatement().execute("SET lock_timeout = '5s'; TRUNCATE orders, outbox RESTART IDENTITY");
        var props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        received.clear();
        topic = new KafkaConsumer<>(props);
        var partitions = topic.partitionsFor(TOPIC).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
        topic.assign(partitions);
        topic.seekToEnd(partitions);
        partitions.forEach(topic::position); // seekToEnd is lazy; force it before the test writes anything
    }

    @AfterEach
    void closeConsumer() {
        topic.close();
    }

    /// Everything published since the test started. Waits up to 15 s for at least
    /// `atLeast` records, then a second more so anything unexpected shows up too.
    static List<ConsumerRecord<String, String>> consume(int atLeast) {
        var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (received.size() < atLeast && System.nanoTime() < deadline) {
            poll();
        }
        var grace = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (System.nanoTime() < grace) {
            poll();
        }
        return received;
    }

    static void poll() {
        topic.poll(Duration.ofMillis(200)).forEach(r -> {
            received.add(r);
            System.out.printf("  topic <- key=%s event_type=%s event_id=%s value=%s%n",
                    r.key(), header(r, "event_type"), header(r, "event_id"), r.value());
        });
    }

    static Set<String> keys(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::key).collect(Collectors.toSet());
    }

    static String header(ConsumerRecord<String, String> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    static String scalar(String sql) throws Exception {
        var rs = db.createStatement().executeQuery(sql);
        rs.next();
        return rs.getString(1);
    }

    static Connection connect() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), "outbox", "outbox");
    }
}
