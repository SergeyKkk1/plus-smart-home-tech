package ru.yandex.practicum.telemetry.collector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import ru.yandex.practicum.kafka.telemetry.serializer.AvroSerializer;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class CollectorApplicationTest {

    private static final String TIMESTAMP = "2024-08-06T16:54:03.129Z";
    private static final long TIMESTAMP_MILLIS = Instant.parse(TIMESTAMP).toEpochMilli();
    private static final String SENSOR = """
            {"id":"sensor.1","hubId":"hub.1","timestamp":"%s",%s}
            """;
    private static final String HUB = """
            {"hubId":"hub.1","timestamp":"%s",%s}
            """;

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper json;

    @MockBean
    private KafkaTemplate<String, SpecificRecordBase> kafka;

    @BeforeEach
    void acknowledgeKafkaWrites() {
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
    }

    @ParameterizedTest(name = "{2} is published as Avro")
    @MethodSource("supportedEvents")
    void publishesEveryEventSubtype(String endpoint, String body, String payloadType,
                                   String expectedPayloadJson) throws Exception {
        MvcResult request = postJson(endpoint, body);
        mvc.perform(asyncDispatch(request)).andExpect(status().isOk());

        ProducerRecord<String, SpecificRecordBase> published = publishedRecord();
        boolean sensor = endpoint.endsWith("sensors");
        assertThat(published.topic()).isEqualTo(sensor ? "telemetry.sensors.v1" : "telemetry.hubs.v1");
        assertThat(published.key()).isEqualTo("hub.1");
        assertThat(published.timestamp()).isEqualTo(TIMESTAMP_MILLIS);

        GenericRecord decoded = decode(published);
        assertThat(decoded.getSchema().getName()).isEqualTo(sensor ? "SensorEventAvro" : "HubEventAvro");
        assertThat(decoded.get(sensor ? "hubId" : "hub_id").toString()).isEqualTo("hub.1");
        assertThat(decoded.get("timestamp")).isEqualTo(TIMESTAMP_MILLIS);
        if (sensor) {
            assertThat(decoded.get("id").toString()).isEqualTo("sensor.1");
        }
        GenericRecord payload = (GenericRecord) decoded.get("payload");
        assertThat(payload.getSchema().getName()).isEqualTo(payloadType);
        Map<String, Object> expectedPayload = json.readValue(expectedPayloadJson, new TypeReference<>() { });
        assertThat(recordFields(payload)).containsAllEntriesOf(expectedPayload);
    }

    @Test
    void defaultsTimestampAndOptionalLightMeasurements() throws Exception {
        long before = Instant.now().toEpochMilli();
        MvcResult request = postJson("/events/sensors", """
                {"id":"sensor.1","hubId":"hub.1","type":"LIGHT_SENSOR_EVENT"}
                """);
        mvc.perform(asyncDispatch(request)).andExpect(status().isOk());
        long after = Instant.now().toEpochMilli();

        ProducerRecord<String, SpecificRecordBase> published = publishedRecord();
        GenericRecord event = decode(published);
        assertThat((Long) event.get("timestamp")).isBetween(before, after);
        assertThat(published.timestamp()).isEqualTo(event.get("timestamp"));
        assertThat(recordFields((GenericRecord) event.get("payload")))
                .containsEntry("link_quality", 0)
                .containsEntry("luminosity", 0);
    }

    @ParameterizedTest
    @MethodSource("invalidEvents")
    void rejectsInvalidEventsBeforePublishing(String endpoint, String body) throws Exception {
        mvc.perform(post(endpoint).contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(kafka);
    }

    @ParameterizedTest(name = "Kafka acknowledgement succeeds: {0}")
    @ValueSource(booleans = {true, false})
    void respondsOnlyAfterKafkaCompletes(boolean success) throws Exception {
        CompletableFuture<SendResult<String, SpecificRecordBase>> acknowledgement = new CompletableFuture<>();
        when(kafka.send(any(ProducerRecord.class))).thenReturn(acknowledgement);

        MvcResult request = postJson("/events/sensors", SENSOR.formatted(TIMESTAMP,
                "\"type\":\"SWITCH_SENSOR_EVENT\",\"state\":true"));
        assertThat(acknowledgement).isNotDone();
        assertThatThrownBy(() -> request.getAsyncResult(0)).isInstanceOf(IllegalStateException.class);

        if (success) {
            acknowledgement.complete(null);
        } else {
            acknowledgement.completeExceptionally(new KafkaException("Broker unavailable"));
        }
        mvc.perform(asyncDispatch(request))
                .andExpect(success ? status().isOk() : status().isServiceUnavailable());
    }

    @Test
    void rejectsPublishingWhenKafkaSendThrowsBeforeReturningFuture() throws Exception {
        when(kafka.send(any(ProducerRecord.class))).thenThrow(new KafkaException("Producer buffer exhausted"));

        mvc.perform(post("/events/sensors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(SENSOR.formatted(TIMESTAMP,
                                "\"type\":\"SWITCH_SENSOR_EVENT\",\"state\":true")))
                .andExpect(status().isServiceUnavailable());
        assertThat(publishedRecord().topic()).isEqualTo("telemetry.sensors.v1");
    }

    private MvcResult postJson(String endpoint, String body) throws Exception {
        return mvc.perform(post(endpoint).contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(request().asyncStarted())
                .andReturn();
    }

    @SuppressWarnings({"unchecked"})
    private ProducerRecord<String, SpecificRecordBase> publishedRecord() {
        ArgumentCaptor<ProducerRecord<String, SpecificRecordBase>> record = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(record.capture());
        return record.getValue();
    }

    private GenericRecord decode(ProducerRecord<String, SpecificRecordBase> record) throws Exception {
        try (AvroSerializer serializer = new AvroSerializer()) {
            byte[] encoded = serializer.serialize(record.topic(), record.value());
            assertThat(encoded).isNotEmpty();
            return new GenericDatumReader<GenericRecord>(record.value().getSchema())
                    .read(null, DecoderFactory.get().binaryDecoder(encoded, null));
        }
    }

    private static Map<String, Object> recordFields(GenericRecord record) {
        Map<String, Object> result = new LinkedHashMap<>();
        record.getSchema().getFields().forEach(field -> result.put(field.name(), normalize(record.get(field.name()))));
        return result;
    }

    private static Object normalize(Object value) {
        if (value instanceof GenericRecord record) {
            return recordFields(record);
        }
        if (value instanceof List<?> items) {
            return items.stream().map(CollectorApplicationTest::normalize).toList();
        }
        if (value instanceof Number || value instanceof Boolean || value == null) {
            return value;
        }
        return value.toString();
    }

    private static Stream<Arguments> supportedEvents() {
        return Stream.of(
                sensor("CLIMATE_SENSOR_EVENT", "\"temperatureC\":23,\"humidity\":55,\"co2Level\":450",
                        "ClimateSensorAvro", "{\"temperature_c\":23,\"humidity\":55,\"co2_level\":450}"),
                sensor("LIGHT_SENSOR_EVENT", "\"linkQuality\":75,\"luminosity\":59",
                        "LightSensorAvro", "{\"link_quality\":75,\"luminosity\":59}"),
                sensor("MOTION_SENSOR_EVENT", "\"linkQuality\":99,\"motion\":true,\"voltage\":3000",
                        "MotionSensorAvro", "{\"link_quality\":99,\"motion\":true,\"voltage\":3000}"),
                sensor("SWITCH_SENSOR_EVENT", "\"state\":false",
                        "SwitchSensorAvro", "{\"state\":false}"),
                sensor("TEMPERATURE_SENSOR_EVENT", "\"temperatureC\":23,\"temperatureF\":73",
                        "TemperatureSensorAvro", "{\"temperature_c\":23,\"temperature_f\":73}"),
                hub("DEVICE_ADDED", "\"id\":\"sensor.1\",\"deviceType\":\"MOTION_SENSOR\"",
                        "DeviceAddedEventAvro", "{\"id\":\"sensor.1\",\"type\":\"MOTION_SENSOR\"}"),
                hub("DEVICE_REMOVED", "\"id\":\"sensor.1\"",
                        "DeviceRemovedEventAvro", "{\"id\":\"sensor.1\"}"),
                hub("SCENARIO_ADDED", """
                        "name":"Evening lights",
                        "conditions":[
                            {"sensorId":"light.1","type":"LUMINOSITY","operation":"LOWER_THAN","value":50},
                            {"sensorId":"motion.1","type":"MOTION","operation":"EQUALS","value":true},
                            {"sensorId":"switch.1","type":"SWITCH","operation":"EQUALS"}
                        ],
                        "actions":[
                            {"sensorId":"lamp.1","type":"ACTIVATE"},
                            {"sensorId":"lamp.2","type":"SET_VALUE","value":42}
                        ]
                        """, "ScenarioAddedEventAvro", """
                        {"name":"Evening lights","conditions":[
                            {"id":"light.1","type":"LUMINOSITY","operation":"LOWER_THAN","value":50},
                            {"id":"motion.1","type":"MOTION","operation":"EQUALS","value":true},
                            {"id":"switch.1","type":"SWITCH","operation":"EQUALS","value":null}
                        ],"actions":[
                            {"sensor_id":"lamp.1","type":"ACTIVATE","value":null},
                            {"sensor_id":"lamp.2","type":"SET_VALUE","value":42}
                        ]}
                        """),
                hub("SCENARIO_REMOVED", "\"name\":\"Evening lights\"",
                        "ScenarioRemovedEventAvro", "{\"name\":\"Evening lights\"}")
        );
    }

    private static Arguments sensor(String type, String fields, String payloadType, String payload) {
        return Arguments.of("/events/sensors", SENSOR.formatted(TIMESTAMP, "\"type\":\"" + type + "\"," + fields),
                payloadType, payload);
    }

    private static Arguments hub(String type, String fields, String payloadType, String payload) {
        return Arguments.of("/events/hubs", HUB.formatted(TIMESTAMP, "\"type\":\"" + type + "\"," + fields),
                payloadType, payload);
    }

    private static Stream<Arguments> invalidEvents() {
        return Stream.of(
                Arguments.of("/events/sensors", """
                        {"id":"sensor.1","hubId":"hub.1","type":"UNKNOWN_EVENT"}
                        """),
                Arguments.of("/events/sensors", """
                        {"id":"sensor.1","hubId":"hub.1","state":true}
                        """),
                Arguments.of("/events/sensors", """
                        {"id":"sensor.1","hubId":"hub.1","type":"SWITCH_SENSOR_EVENT"}
                        """),
                Arguments.of("/events/hubs", """
                        {"hubId":" ","type":"DEVICE_REMOVED","id":"sensor.1"}
                        """),
                Arguments.of("/events/hubs", """
                        {"hubId":"hub.1","type":"SCENARIO_ADDED","name":"Evening lights",
                         "conditions":[],"actions":[]}
                        """)
        );
    }
}
