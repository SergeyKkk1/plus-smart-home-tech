package ru.yandex.practicum.telemetry.collector.service;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.telemetry.collector.model.ClimateSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.DeviceAction;
import ru.yandex.practicum.telemetry.collector.model.DeviceAddedEvent;
import ru.yandex.practicum.telemetry.collector.model.DeviceRemovedEvent;
import ru.yandex.practicum.telemetry.collector.model.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.LightSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.MotionSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.ScenarioAddedEvent;
import ru.yandex.practicum.telemetry.collector.model.ScenarioCondition;
import ru.yandex.practicum.telemetry.collector.model.ScenarioRemovedEvent;
import ru.yandex.practicum.telemetry.collector.model.SensorEvent;
import ru.yandex.practicum.telemetry.collector.model.SwitchSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.TemperatureSensorEvent;

@Component
public class EventMapper {
    public SensorEventAvro map(SensorEvent event) {
        Object payload = switch (event) {
            case ClimateSensorEvent climate -> new ClimateSensorAvro(
                    climate.getTemperatureC(), climate.getHumidity(), climate.getCo2Level());
            case LightSensorEvent light -> new LightSensorAvro(light.getLinkQuality(), light.getLuminosity());
            case MotionSensorEvent motion -> new MotionSensorAvro(
                    motion.getLinkQuality(), motion.getMotion(), motion.getVoltage());
            case SwitchSensorEvent state -> new SwitchSensorAvro(state.getState());
            case TemperatureSensorEvent temperature -> new TemperatureSensorAvro(
                    temperature.getTemperatureC(), temperature.getTemperatureF());
            default -> throw new IllegalArgumentException("Unsupported sensor event: " + event.getType());
        };
        return new SensorEventAvro(event.getId(), event.getHubId(), event.getTimestamp(), payload);
    }

    public HubEventAvro map(HubEvent event) {
        Object payload = switch (event) {
            case DeviceAddedEvent added -> new DeviceAddedEventAvro(
                    added.getId(), DeviceTypeAvro.valueOf(added.getDeviceType().name()));
            case DeviceRemovedEvent removed -> new DeviceRemovedEventAvro(removed.getId());
            case ScenarioAddedEvent added -> new ScenarioAddedEventAvro(
                    added.getName(),
                    added.getConditions().stream().map(this::mapCondition).toList(),
                    added.getActions().stream().map(this::mapAction).toList());
            case ScenarioRemovedEvent removed -> new ScenarioRemovedEventAvro(removed.getName());
            default -> throw new IllegalArgumentException("Unsupported hub event: " + event.getType());
        };
        return new HubEventAvro(event.getHubId(), event.getTimestamp(), payload);
    }

    private ScenarioConditionAvro mapCondition(ScenarioCondition condition) {
        return new ScenarioConditionAvro(condition.getSensorId(), ConditionTypeAvro.valueOf(condition.getType().name()),
                ConditionOperationAvro.valueOf(condition.getOperation().name()), condition.getValue());
    }

    private DeviceActionAvro mapAction(DeviceAction action) {
        return new DeviceActionAvro(action.getSensorId(), ActionTypeAvro.valueOf(action.getType().name()), action.getValue());
    }
}
