package io.kestra.scheduler.events;

import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.TriggerId;

import java.time.Instant;

/**
 * A trigger execution completed.
 */
public record TriggerCompleted(
    TriggerId id,
    Instant timestamp,
    String executionId,
    State.Type executionState
) implements TriggerEvent {
    
}
