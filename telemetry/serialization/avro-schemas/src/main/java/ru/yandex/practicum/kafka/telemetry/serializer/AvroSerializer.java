package ru.yandex.practicum.kafka.telemetry.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class AvroSerializer implements Serializer<SpecificRecordBase> {
    private final ConcurrentMap<Class<?>, SpecificDatumWriter<SpecificRecordBase>> writers =
            new ConcurrentHashMap<>();

    @Override
    public byte[] serialize(String topic, SpecificRecordBase event) {
        if (event == null) {
            return null;
        }

        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            SpecificDatumWriter<SpecificRecordBase> writer = writers.computeIfAbsent(event.getClass(),
                    ignored -> new SpecificDatumWriter<>(event.getSchema(), event.getSpecificData()));
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            writer.write(event, encoder);
            encoder.flush();
            return output.toByteArray();
        } catch (IOException | RuntimeException exception) {
            throw new SerializationException("Cannot serialize Avro event for topic " + topic, exception);
        }
    }
}
