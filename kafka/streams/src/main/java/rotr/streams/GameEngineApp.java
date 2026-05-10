package rotr.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class GameEngineApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());

        consumer.subscribe(Collections.singletonList("game.orders.validated"));

        System.out.println("GameEngineApp started. Listening to game.orders.validated...");

        while (true) {
            var records = consumer.poll(Duration.ofMillis(500));

            records.forEach(record -> {
                try {
                    OrderRecord order = MAPPER.readValue(record.value(), OrderRecord.class);

if ("ASSIGN_ROUTE".equals(order.orderType)) {
    UnitStateRecord unit = GameUnitRegistry.getUnit(order.unitId);

    if (unit == null) {
        System.out.println("Unknown unit: " + order.unitId);
        return;
    }

    OrderPayload payload = MAPPER.readValue(order.payload, OrderPayload.class);

    if (payload.pathIds != null) {
        unit.route = payload.pathIds;
        unit.routeIdx = 0;
    }

    String eventJson = MAPPER.writeValueAsString(unit);

    ProducerRecord<String, String> event =
            new ProducerRecord<>("game.events.unit", order.unitId, eventJson);

    producer.send(event);

    System.out.println("Produced unit event for " + order.unitId + ": " + eventJson);
}
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "game-engine-app");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
