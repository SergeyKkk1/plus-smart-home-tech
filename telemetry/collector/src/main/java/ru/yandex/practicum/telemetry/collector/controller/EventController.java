package ru.yandex.practicum.telemetry.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.telemetry.collector.model.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.EventService;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
public class EventController {
    private final EventService eventService;

    @PostMapping("/sensors")
    public CompletableFuture<Void> collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        return eventService.collect(event);
    }

    @PostMapping("/hubs")
    public CompletableFuture<Void> collectHubEvent(@Valid @RequestBody HubEvent event) {
        return eventService.collect(event);
    }
}
