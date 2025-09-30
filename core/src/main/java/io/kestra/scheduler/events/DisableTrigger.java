package io.kestra.scheduler.events;

import io.kestra.core.models.triggers.TriggerId;

import java.time.Instant;

/**
 * A command to disable/enable a trigger.
 */
public record DisableTrigger(
    TriggerId id,
    Instant timestamp,
    Boolean disabled
) implements TriggerEvent {
    
}
