package ru.yandex.practicum.telemetry.collector.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("collector.kafka.topics")
public record KafkaTopics(@NotBlank String sensors, @NotBlank String hubs) {
}
