package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import ru.yandex.practicum.telemetry.collector.config.KafkaTopics;
import ru.yandex.practicum.telemetry.collector.model.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.SensorEvent;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventService {
    private final EventMapper mapper;
    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;
    private final KafkaTopics topics;

    public CompletableFuture<Void> collect(SensorEvent event) {
        return publish(topics.sensors(), event.getHubId(), event.getTimestamp(), mapper.map(event));
    }

    public CompletableFuture<Void> collect(HubEvent event) {
        return publish(topics.hubs(), event.getHubId(), event.getTimestamp(), mapper.map(event));
    }

    private CompletableFuture<Void> publish(String topic, String hubId, Instant timestamp,
                                            SpecificRecordBase event) {
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(topic, null, timestamp.toEpochMilli(), hubId, event);
        try {
            return kafkaTemplate.send(record).handle((result, failure) -> {
                if (failure != null) {
                    throw publishingFailure(topic, hubId, failure);
                }
                return null;
            });
        } catch (RuntimeException failure) {
            throw publishingFailure(topic, hubId, failure);
        }
    }

    private ResponseStatusException publishingFailure(String topic, String hubId, Throwable failure) {
        log.error("Failed to publish event to {} for hub {}", topic, hubId, failure);
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Event could not be published", failure);
    }
}
