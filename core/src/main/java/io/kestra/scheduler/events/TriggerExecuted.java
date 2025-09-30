package io.kestra.scheduler.events;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.TriggerId;

import java.time.Instant;

/**
 * A trigger was executed.
 */
public record TriggerExecuted(
    TriggerId id,
    Instant timestamp,
    // TODO we could have a dedicated class to simplify the model
    Execution execution
) implements TriggerEvent {
    
}
