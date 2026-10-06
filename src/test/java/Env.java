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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/// The plumbing: real Postgres (logical decoding on), real Kafka, real Debezium
/// Connect, a consumer parked at the end of the topic. Nothing here is the pattern;
/// the pattern is in DebeziumOutboxTest.
class Env {

    static final String TOPIC = "order.events";
    static final Network NET = Network.newNetwork();

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6-alpine")
            .withNetwork(NET).withNetworkAliases("postgres")
            .withUsername("outbox").withPassword("outbox").withDatabaseName("outbox")
            .withCommand("postgres", "-c", "wal_level=logical")
            .withCopyFileToContainer(MountableFile.forHostPath("schema.sql"), "/docker-entrypoint-initdb.d/schema.sql");

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1")
            .withNetwork(NET).withNetworkAliases("kafka").withListener("kafka:19092");

    static final GenericContainer<?> CONNECT = new GenericContainer<>("quay.io/debezium/connect:3.3.2.Final")
            .withNetwork(NET).dependsOn(POSTGRES, KAFKA)
            .withEnv(Map.of(
                    "BOOTSTRAP_SERVERS", "kafka:19092",
                    "GROUP_ID", "connect",
                    "CONFIG_STORAGE_TOPIC", "connect_configs",
                    "OFFSET_STORAGE_TOPIC", "connect_offsets",
                    "STATUS_STORAGE_TOPIC", "connect_statuses"))
            .withExposedPorts(8083)
            .waitingFor(Wait.forHttp("/connectors").forPort(8083));

    static Connection db;

    /// Three partitions so the "same aggregate, same partition" test means something.
    @BeforeAll
    static void start() throws Exception {
        Startables.deepStart(CONNECT).join();
        try (var admin = Admin.create(Map.of("bootstrap.servers", (Object) KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
        }
        db = connect();
        registerConnector();
    }

    static Connection connect() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), "outbox", "outbox");
    }

    // --- kafka connect ------------------------------------------------------------

    static final HttpClient HTTP = HttpClient.newHttpClient();
    static final String CONNECTOR = "/connectors/debezium-connector";

    static void registerConnector() throws Exception {
        var put = request(CONNECTOR + "/config")
                .header("content-type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(Files.readString(Path.of("connector.json")))).build();
        var res = HTTP.send(put, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) throw new IllegalStateException("register: " + res.body());

        var running = "\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"";
        var status = request(CONNECTOR + "/status").GET().build();
        var deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        String body;
        do {
            Thread.sleep(500);
            body = HTTP.send(status, HttpResponse.BodyHandlers.ofString()).body();
        } while (!body.contains(running) && System.nanoTime() < deadline);
        if (!body.contains(running)) throw new IllegalStateException("task never started: " + body);
        System.out.println("  connect <- " + body);
    }

    static void deleteConnector() throws Exception {
        var res = HTTP.send(request(CONNECTOR).DELETE().build(), HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) throw new IllegalStateException("delete: " + res.body());
        // DELETE returns before the task stops; the slot going inactive is the real signal
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!"0".equals(scalar(SLOT_ACTIVE)) && System.nanoTime() < deadline) {
            Thread.sleep(200);
        }
        if (!"0".equals(scalar(SLOT_ACTIVE))) throw new IllegalStateException("task never stopped");
    }

    static final String SLOT_ACTIVE = "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'outbox_slot' AND active";

    static HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083) + path));
    }

    // --- writing the way the application does --------------------------------------

    /// The orders row and its outbox row in ONE statement: atomic by construction.
    static String insertOrder(Connection c, String customer) throws Exception {
        return scalar(c, """
                WITH o AS (INSERT INTO orders (id, customer_id, amount_cents)
                           VALUES (gen_random_uuid(), '%s', 1000) RETURNING *)
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                SELECT gen_random_uuid(), o.id, 'order.created', to_jsonb(o) FROM o
                RETURNING aggregate_id""".formatted(customer));
    }

    /// Same shape for a change to an existing order.
    static void updateOrder(Connection c, String orderId) throws Exception {
        scalar(c, """
                WITH o AS (UPDATE orders SET amount_cents = 2000 WHERE id = '%s' RETURNING *)
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                SELECT gen_random_uuid(), o.id, 'order.updated', to_jsonb(o) FROM o
                RETURNING aggregate_id""".formatted(orderId));
    }

    static String scalar(String sql) throws Exception {
        return scalar(db, sql);
    }

    static String scalar(Connection c, String sql) throws Exception {
        var rs = c.createStatement().executeQuery(sql);
        rs.next();
        return rs.getString(1);
    }

    // --- reading the topic -----------------------------------------------------------

    static KafkaConsumer<String, String> topic;
    static final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    /// Empty tables, and a consumer parked at the end of the topic so each test
    /// reads back only what it produced.
    @BeforeEach
    void startClean() throws Exception {
        db.createStatement().execute("TRUNCATE orders, outbox RESTART IDENTITY");
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
            System.out.printf("  topic <- partition=%d key=%s event_type=%s event_id=%s value=%s%n",
                    r.partition(), r.key(), header(r, "event_type"), header(r, "event_id"), r.value());
        });
    }

    static List<String> keys(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::key).toList();
    }

    static Set<String> partitions(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(r -> String.valueOf(r.partition())).collect(Collectors.toSet());
    }

    static String header(ConsumerRecord<String, String> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
