import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Timestamp;
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

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(
                        "boiler-sensors",
                        new SimpleStringSchema(),
                        props
                );

        var stream = env.addSource(consumer);

        ObjectMapper mapper = new ObjectMapper();

        stream.addSink(
                JdbcSink.sink(

                        "INSERT INTO sensor_timeseries " +
                                "(event_time, pi_point_id, value, quality) " +
                                "VALUES (?, ?, ?, ?)",

                        (ps, json) -> {

                            try {

                                JsonNode node = mapper.readTree(json);

                                int id = node.get("pi_point_id").asInt();
                                double value = node.get("value").asDouble();
                                int quality = node.get("quality").asInt();

                                String ts = node.get("timestamp").asText();

                                Timestamp t =
                                        Timestamp.from(
                                                java.time.Instant.parse(
                                                        ts.replace("+05:30", "Z")
                                                )
                                        );

                                ps.setTimestamp(1, t);
                                ps.setInt(2, id);
                                ps.setDouble(3, value);
                                ps.setInt(4, quality);

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