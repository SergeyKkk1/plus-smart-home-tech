package ru.yandex.practicum.kafka.telemetry.serializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

class AvroSerializerTest {
    private final AvroSerializer serializer = new AvroSerializer();

    @Test
    void roundTripsSensorWithLogicalTimestamp() throws IOException {
        SensorEventAvro event = new SensorEventAvro("sensor-1", "hub-1",
                Instant.parse("2026-10-28T12:00:00.123Z"), new LightSensorAvro(90, 320));

        assertEquals(event, roundTrip(event, SensorEventAvro.getClassSchema()));
    }

    @Test
    void roundTripsScenarioWithEveryConditionValueAndNullableAction() throws IOException {
        ScenarioConditionAvro unsetCondition = ScenarioConditionAvro.newBuilder()
                .setId("sensor-1")
                .setType(ConditionTypeAvro.MOTION)
                .setOperation(ConditionOperationAvro.EQUALS)
                .build();
        DeviceActionAvro action = DeviceActionAvro.newBuilder()
                .setSensorId("switch-1")
                .setType(ActionTypeAvro.ACTIVATE)
                .build();
        List<ScenarioConditionAvro> conditions = List.of(unsetCondition,
                new ScenarioConditionAvro("sensor-2", ConditionTypeAvro.LUMINOSITY,
                        ConditionOperationAvro.LOWER_THAN, 20),
                new ScenarioConditionAvro("sensor-3", ConditionTypeAvro.SWITCH,
                        ConditionOperationAvro.EQUALS, true));
        HubEventAvro event = new HubEventAvro("hub-1", Instant.parse("2026-10-28T12:00:00Z"),
                new ScenarioAddedEventAvro("Evening", conditions, List.of(action)));

        assertNull(unsetCondition.getValue());
        assertNull(action.getValue());
        assertEquals(event, roundTrip(event, HubEventAvro.getClassSchema()));
    }

    @Test
    void supportsTombstonesAndWrapsInvalidRecordFailures() {
        assertNull(serializer.serialize("telemetry.sensors.v1", null));
        assertThrows(SerializationException.class,
                () -> serializer.serialize("telemetry.sensors.v1", new SensorEventAvro()));
    }

    private <T extends SpecificRecordBase> T roundTrip(T event, Schema schema) throws IOException {
        byte[] bytes = serializer.serialize("telemetry", event);
        return new SpecificDatumReader<T>(schema)
                .read(null, DecoderFactory.get().binaryDecoder(bytes, null));
    }
}
