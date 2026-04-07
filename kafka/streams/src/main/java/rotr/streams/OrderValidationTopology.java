package rotr.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class OrderValidationTopology {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int CURRENT_TURN = 1;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-validation-topology");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-order-validation");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawOrders = builder.stream("game.orders.raw");

        KStream<String, ValidationResult>[] branches = rawOrders
                .mapValues(OrderValidationTopology::validateOrder)
                .branch(
                        (key, value) -> value.valid,
                        (key, value) -> !value.valid
                );

        KStream<String, ValidationResult> validOrders = branches[0];
        KStream<String, ValidationResult> invalidOrders = branches[1];

        validOrders
                .mapValues(v -> v.originalJson)
                .to("game.orders.validated", Produced.with(Serdes.String(), Serdes.String()));

        invalidOrders
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

    private static ValidationResult validateOrder(String rawJson) {
        try {
            OrderRecord order = MAPPER.readValue(rawJson, OrderRecord.class);

            if (order.playerId == null || order.playerId.isBlank()
                    || order.unitId == null || order.unitId.isBlank()
                    || order.orderType == null || order.orderType.isBlank()) {
                return ValidationResult.invalid(
                        new DLQRecord(
                                "game.orders.raw",
                                0,
                                0,
                                "INVALID_ORDER",
                                "Missing required fields",
                                rawJson,
                                System.currentTimeMillis()
                        )
                );
            }

            if (order.turn != CURRENT_TURN) {
                return ValidationResult.invalid(
                        new DLQRecord(
                                "game.orders.raw",
                                0,
                                0,
                                "WRONG_TURN",
                                "Order turn does not match current turn",
                                rawJson,
                                System.currentTimeMillis()
                        )
                );
            }

            return ValidationResult.valid(rawJson);

        } catch (Exception e) {
            return ValidationResult.invalid(
                    new DLQRecord(
                            "game.orders.raw",
                            0,
                            0,
                            "DESERIALIZATION_ERROR",
                            e.getMessage(),
                            rawJson,
                            System.currentTimeMillis()
                    )
            );
        }
    }

    static class ValidationResult {
        boolean valid;
        String originalJson;
        DLQRecord dlqRecord;

        static ValidationResult valid(String json) {
            ValidationResult r = new ValidationResult();
            r.valid = true;
            r.originalJson = json;
            return r;
        }

        static ValidationResult invalid(DLQRecord dlq) {
            ValidationResult r = new ValidationResult();
            r.valid = false;
            r.dlqRecord = dlq;
            return r;
        }
    }
}