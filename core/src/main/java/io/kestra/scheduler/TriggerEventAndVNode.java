package io.kestra.scheduler;

import io.kestra.scheduler.events.TriggerEvent;

import java.util.Objects;

/**
 * Wraps a {@link TriggerEvent} with the associated Virtual Node (vNodes).
 *
 * @param event the trigger event.
 * @param vNode the virtual node.
 */
public record TriggerEventAndVNode(
    TriggerEvent event, 
    Integer vNode) {
    
    public TriggerEventAndVNode {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(vNode, "vNode must not be null");
    }
}
