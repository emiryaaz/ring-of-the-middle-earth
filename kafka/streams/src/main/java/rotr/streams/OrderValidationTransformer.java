package rotr.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.state.KeyValueStore;

public class OrderValidationTransformer implements ValueTransformerWithKey<String, String, OrderValidationTopology.ValidationResult> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int CURRENT_TURN = 1;

    private final String turnStoreName;
    private final String unitStoreName;
    private final String dedupStoreName;

    private ProcessorContext context;
    private KeyValueStore<String, String> dedupStore;

    public OrderValidationTransformer(String turnStoreName, String unitStoreName, String dedupStoreName) {
        this.turnStoreName = turnStoreName;
        this.unitStoreName = unitStoreName;
        this.dedupStoreName = dedupStoreName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        this.dedupStore = (KeyValueStore<String, String>) context.getStateStore(dedupStoreName);
    }

    @Override
    public OrderValidationTopology.ValidationResult transform(String readOnlyKey, String rawJson) {
        try {
            OrderRecord order = MAPPER.readValue(rawJson, OrderRecord.class);

            if (order.playerId == null || order.playerId.isBlank()
                    || order.unitId == null || order.unitId.isBlank()
                    || order.orderType == null || order.orderType.isBlank()) {
                return invalid("INVALID_ORDER", "Missing required fields", rawJson);
            }

            if (order.turn != CURRENT_TURN) {
                return invalid("WRONG_TURN", "Order turn does not match current turn", rawJson);
            }

UnitStateRecord unit = GameUnitRegistry.getUnit(order.unitId);

if (unit != null) {

    if (!unit.side.equals(order.playerId)) {
        return invalid("NOT_YOUR_UNIT", "Unit does not belong to submitting player", rawJson);
    }

    boolean specialAbility =
            order.orderType.equals("CAST_SPELL") ||
            order.orderType.equals("SPECIAL_ATTACK");

    if (specialAbility && unit.cooldown > 0) {
        return invalid(
                "ABILITY_ON_COOLDOWN",
                "Unit ability is currently on cooldown",
                rawJson
        );
    }

}

if ("BLOCK_PATH".equals(order.orderType)) {
    OrderPayload payload = MAPPER.readValue(order.payload, OrderPayload.class);

    if (payload.pathId == null || !PathRegistry.exists(payload.pathId)) {
        return invalid("INVALID_PATH", "Path does not exist", rawJson);
    }

    if (unit == null || unit.region == null || !PathRegistry.isEndpoint(payload.pathId, unit.region)) {
        return invalid(
                "UNIT_NOT_ADJACENT",
                "Unit is not located at one of the path endpoint regions",
                rawJson
        );
    }
}

            String duplicateKey = "turn:" + order.turn + ":unit:" + order.unitId;

            if (dedupStore.get(duplicateKey) != null) {
                return invalid("DUPLICATE_UNIT_ORDER", "Same unit already has an order this turn", rawJson);
            }

            dedupStore.put(duplicateKey, "seen");

            return OrderValidationTopology.ValidationResult.valid(rawJson);

        } catch (Exception e) {
            return invalid("DESERIALIZATION_ERROR", e.getMessage(), rawJson);
        }
    }

    private OrderValidationTopology.ValidationResult invalid(String code, String message, String rawJson) {
        return OrderValidationTopology.ValidationResult.invalid(
                new DLQRecord(
                        "game.orders.raw",
                        context.partition(),
                        context.offset(),
                        code,
                        message,
                        rawJson,
                        System.currentTimeMillis()
                )
        );
    }

    @Override
    public void close() {
    }
}
