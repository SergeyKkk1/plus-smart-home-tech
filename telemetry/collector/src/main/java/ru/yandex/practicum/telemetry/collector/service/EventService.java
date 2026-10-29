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
        log.info("Collecting sensor event {} from sensor {} for hub {} at {}",
                event.getType(), event.getId(), event.getHubId(), event.getTimestamp());
        return publish(topics.sensors(), event.getHubId(), event.getTimestamp(), mapper.map(event));
    }

    public CompletableFuture<Void> collect(HubEvent event) {
        log.info("Collecting hub event {} for hub {} at {}",
                event.getType(), event.getHubId(), event.getTimestamp());
        return publish(topics.hubs(), event.getHubId(), event.getTimestamp(), mapper.map(event));
    }

    private CompletableFuture<Void> publish(String topic, String hubId, Instant timestamp,
                                            SpecificRecordBase event) {
        log.info("Publishing event to {} for hub {} at {}", topic, hubId, timestamp);
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(topic, null, timestamp.toEpochMilli(), hubId, event);
        try {
            return kafkaTemplate.send(record).handle((result, failure) -> {
                if (failure != null) {
                    throw publishingFailure(topic, hubId, failure);
                }
                log.info("Published event to {} for hub {} at {}", topic, hubId, timestamp);
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
