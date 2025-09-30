package io.kestra.scheduler.events;

import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.TriggerId;

import java.time.Instant;
import java.util.List;

/**
 * A new trigger was created (i.e. added to a flow).
 */
public record TriggerCreated(
    TriggerId id,
    Instant timestamp,
    Boolean disabled,
    List<State.Type> stopAfter
) implements TriggerEvent {
    
}
