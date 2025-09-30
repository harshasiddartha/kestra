package io.kestra.scheduler.events;

import io.kestra.core.models.triggers.TriggerId;

import java.time.Instant;

/**
 * An existing trigger was updated.
 */
public record TriggerUpdated(
    TriggerId id,
    Instant timestamp
) implements TriggerEvent {
    
}
