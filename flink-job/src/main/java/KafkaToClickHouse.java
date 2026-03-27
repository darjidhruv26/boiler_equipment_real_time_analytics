import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaToClickHouse {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:29092");
        props.setProperty("group.id", "flink-boiler");

        List<String> topics = Arrays.asList("boiler-sensors", "turbine", "generator", "condenser", "cooling-tower", "coal-handling", "ash-handling", "water-treatment", "electrical-system", "instrumentation-control");
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(
                        topics,
                        new SimpleStringSchema(),
                        props
                );

        var stream = env.addSource(consumer);

        ObjectMapper mapper = new ObjectMapper();

        stream.addSink(
                JdbcSink.sink(

                        "INSERT INTO tag_timeseries " +
                                "(event_time, pi_point_id, equipment_id, value, quality, kafka_topic, partition_id) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?)",

                        (ps, json) -> {

                            try {

                                JsonNode node = mapper.readTree(json);

                                int id = node.get("pi_point_id").asInt();
                                String equipmentId = node.has("equipment_id") ? node.get("equipment_id").asText() : "UNKNOWN";
                                double value = node.get("value").asDouble();
                                int quality = node.get("quality").asInt();
                                String topic = node.has("topic") ? node.get("topic").asText() : "unknown";
                                int partitionId = node.has("partition") ? node.get("partition").asInt() : 0;

                                // Support both event_time and older timestamp formats
                                String ts = node.has("event_time") ? node.get("event_time").asText() : 
                                                (node.has("timestamp") ? node.get("timestamp").asText() : java.time.Instant.now().toString());

                                Timestamp t =
                                        Timestamp.from(
                                                java.time.Instant.parse(
                                                        ts.endsWith("Z") ? ts : ts + "Z"
                                                )
                                        );

                                ps.setTimestamp(1, t);
                                ps.setInt(2, id);
                                ps.setString(3, equipmentId);
                                ps.setDouble(4, value);
                                ps.setInt(5, quality);
                                ps.setString(6, topic);
                                ps.setInt(7, partitionId);

                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        },

                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(2000)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://clickhouse:8123/industrial_analytics?user=admin&password=admin")
                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                .withUsername("admin")
                                .withPassword("admin")
                                .build()
                )
        );

        env.execute("Kafka → ClickHouse");
    }
}