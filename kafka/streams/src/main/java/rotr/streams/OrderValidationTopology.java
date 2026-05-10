package rotr.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class OrderValidationTopology {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DEDUP_STORE = "unit-order-store";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-validation-topology");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-order-validation");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(
                org.apache.kafka.streams.state.Stores.keyValueStoreBuilder(
                        org.apache.kafka.streams.state.Stores.persistentKeyValueStore(DEDUP_STORE),
                        Serdes.String(),
                        Serdes.String()
                )
        );

        KTable<String, String> turnTable = builder.table("game.session");
        KTable<String, String> unitTable = builder.table("game.events.unit");

        KStream<String, String> rawOrders = builder.stream("game.orders.raw");

        KStream<String, ValidationResult> validationResults = rawOrders.transformValues(
                () -> new OrderValidationTransformer(
                        turnTable.queryableStoreName(),
                        unitTable.queryableStoreName(),
                        DEDUP_STORE
                ),
                DEDUP_STORE
        );

        KStream<String, ValidationResult>[] branches = validationResults.branch(
                (key, value) -> value.valid,
                (key, value) -> !value.valid
        );

        branches[0]
                .mapValues(v -> v.originalJson)
                .to("game.orders.validated", Produced.with(Serdes.String(), Serdes.String()));

        branches[1]
                .mapValues(v -> {
                    try {
                        return MAPPER.writeValueAsString(v.dlqRecord);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .to("game.dlq", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }

    static class ValidationResult {
        boolean valid;
        String originalJson;
        DLQRecord dlqRecord;

        static ValidationResult valid(String json) {
            ValidationResult result = new ValidationResult();
            result.valid = true;
            result.originalJson = json;
            return result;
        }

        static ValidationResult invalid(DLQRecord dlq) {
            ValidationResult result = new ValidationResult();
            result.valid = false;
            result.dlqRecord = dlq;
            return result;
        }
    }
}
