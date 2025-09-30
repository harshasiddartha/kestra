package io.kestra.scheduler;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.utils.Disposable;
import io.kestra.scheduler.events.TriggerEvent;

import java.io.Closeable;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A service interface for publishing and subscribing to the {@link TriggerEvent} queue.
 */
public interface TriggerEventQueue extends Closeable {
    
    void send(TriggerEvent triggerEvent);
    
    Disposable subscribe(Subscription subscription, BiConsumer<Integer, TriggerEvent> handler) throws SubscriptionBusyException;
    
    /**
     * Represents a queue subscription.
     * 
     * @param name      the subscription name.
     * @param vNodes    the subscription virtual nodes.
     */
    record Subscription(String name, Set<Integer> vNodes) {

    }
    
    class SubscriptionBusyException extends KestraRuntimeException {
        
        public SubscriptionBusyException(String queue, String subscriptionName, Integer vNode) {
            super("Exclusive consumer is already connected [queue=%s, subscriptionName=%s, vNode=%s]"
                .formatted(queue, subscriptionName, vNode)
            );
        }
    }
}
