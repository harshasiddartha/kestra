package io.kestra.scheduler.pubsub;

import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.TriggerId;
import io.kestra.core.models.triggers.WorkerTriggerInterface;
import io.kestra.core.services.FlowListenersInterface;
import io.kestra.core.services.FlowService;
import io.kestra.core.utils.ListUtils;
import io.kestra.scheduler.TriggerEventQueue;
import io.kestra.scheduler.events.TriggerCreated;
import io.kestra.scheduler.events.TriggerDeleted;
import io.kestra.scheduler.events.TriggerEvent;
import io.kestra.scheduler.events.TriggerUpdated;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Instant;
import java.util.List;

// TODO - probably a temporary class (FlowListenersInterface should be removed at some point)
@Singleton
public class TriggerEventPublisher implements Runnable {
    
    private final FlowListenersInterface flowListeners;
    private final TriggerEventQueue triggerEventQueue;
    
    @Inject
    public TriggerEventPublisher(FlowListenersInterface flowListeners,
                                 TriggerEventQueue triggerEventQueue) {
        this.flowListeners = flowListeners;
        this.triggerEventQueue = triggerEventQueue;
    }
    
    @PostConstruct
    @Override
    public void run() {
        this.flowListeners.listen((flow, previous) -> {
            
            if (flow.isDeleted() || previous != null) {
                List<AbstractTrigger> triggersDeleted = flow.isDeleted() ?
                    ListUtils.emptyOnNull(flow.getTriggers()) :
                    FlowService.findRemovedTrigger(flow, previous);
                
                triggersDeleted.forEach(trigger ->
                    sendEvent(new TriggerDeleted(TriggerId.of(flow, trigger), Instant.now()))
                );
            }
            
            if (previous != null) {
                FlowService.findUpdatedTrigger(flow, previous)
                    .stream().filter(trigger -> trigger instanceof WorkerTriggerInterface)
                    .forEach(trigger ->
                        sendEvent(new TriggerUpdated(TriggerId.of(flow, trigger), Instant.now()))
                    );
            }
            
            if (previous == null) {
                flow.getTriggers()
                    .stream().filter(trigger -> trigger instanceof WorkerTriggerInterface)
                    .forEach(trigger ->
                        sendEvent(new TriggerCreated(TriggerId.of(flow, trigger), Instant.now(), trigger.isDisabled(), trigger.getStopAfter()))
                    );
            }
        });
    }
    
    private void sendEvent(TriggerEvent event) {
        this.triggerEventQueue.send(event);
    }
}
