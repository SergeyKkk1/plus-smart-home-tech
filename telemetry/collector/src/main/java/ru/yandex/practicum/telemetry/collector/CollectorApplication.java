package ru.yandex.practicum.telemetry.collector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import ru.yandex.practicum.telemetry.collector.config.KafkaTopics;

@SpringBootApplication
@EnableConfigurationProperties(KafkaTopics.class)
public class CollectorApplication {
    public static void main(String[] args) {
        SpringApplication.run(CollectorApplication.class, args);
    }
}
