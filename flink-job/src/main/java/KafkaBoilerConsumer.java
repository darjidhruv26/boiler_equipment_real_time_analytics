import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaBoilerConsumer {

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

        env.addSource(consumer).print();

        env.execute("Boiler Sensor Stream");
    }
}