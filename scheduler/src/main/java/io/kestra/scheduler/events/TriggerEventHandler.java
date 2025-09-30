package io.kestra.scheduler.events;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.scheduler.internals.NextEvaluationDate;
import io.kestra.scheduler.models.TriggerState;
import io.kestra.scheduler.models.TriggerStatus;
import io.kestra.scheduler.pubsub.TriggerExecutionPublisher;
import io.kestra.scheduler.stores.FlowStateStore;
import io.kestra.scheduler.stores.TriggerStateStore;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Optional;

/**
 * A service for handling {@link TriggerEvent}.
 */
@Singleton
public class TriggerEventHandler {
    
    private static final Logger log = LoggerFactory.getLogger(TriggerEventHandler.class);
    
    private final TriggerStateStore triggerStateStore;
    private final FlowStateStore flowStateStore;
    private final TriggerExecutionPublisher triggerExecutionPublisher;
    
    @Inject
    public TriggerEventHandler(TriggerStateStore triggerStateStore,
                               FlowStateStore flowStateStore,
                               TriggerExecutionPublisher triggerExecutionPublisher) {
        this.triggerStateStore = triggerStateStore;
        this.flowStateStore = flowStateStore;
        this.triggerExecutionPublisher = triggerExecutionPublisher;
    }
    
    public void handle(Clock clock, Integer vNode, TriggerEvent event) {
        log.info("Received trigger event: {}", event);
        switch (event) {
            case TriggerCreated triggerCreated -> {
                onTriggerCreated(triggerCreated, vNode);
            }
            case TriggerDeleted triggerDeleted -> {
                onTriggerDeleted(triggerDeleted);
            }
            case TriggerUpdated triggerUpdated -> {
            }
            case ResetTrigger triggerReset -> {
            }
            case TriggerCompleted triggerCompleted -> {
                onTriggerCompleted(clock, triggerCompleted);
            }
            case TriggerExecuted triggerExecuted -> {
                onTriggerExecuted(clock, triggerExecuted);
            }
            default -> throw new IllegalStateException("Unexpected value: " + event);
        }
    }
    
    public void onTriggerCompleted(Clock clock, TriggerCompleted event) {
        TriggerState newState = triggerStateStore.find(event.id()).orElseThrow();
        newState = newState
            .status(clock, TriggerStatus.IDLE)
            .updateForExecutionState(clock, event.executionState());
        triggerStateStore.save(newState);
    }
    
    public void onTriggerExecuted(Clock clock, TriggerExecuted event) {
        TriggerState triggerState = triggerStateStore.find(event.id()).orElseThrow();// TODO
        
        FlowWithSource flowWithSource = flowStateStore.findFlow(event.id().getTenantId(), event.id().getNamespace(), event.id().getFlowId()).orElseThrow();// TODO
        
        Optional<AbstractTrigger> trigger = flowWithSource.getTriggers().stream()
            .filter(it -> it.getId().equals(event.id().getTriggerId()))
            .findFirst();
        
        TriggerState newState = triggerState;
        if (trigger.isPresent()) {
            newState = triggerState
                .updateForNextEvaluationDate(clock, NextEvaluationDate.get(clock, trigger.get()));
        }
        
        if (event.execution() != null) {
            newState.updateForExecution(clock, event.execution());
        }
        
        triggerStateStore.save(newState);
        
        if (event.execution() != null) {
            Execution execution = event.execution().withTenantId(triggerState.getTenantId());
            triggerExecutionPublisher.sendExecution(execution);
        }
    }
    
    public void onTriggerDeleted(TriggerDeleted event) {
        triggerStateStore.delete(event.id());
    }
    
    public void onTriggerCreated(TriggerCreated triggerCreated, Integer vNode) {
        TriggerState newState = TriggerState.of(triggerCreated.id(), triggerCreated.stopAfter(), triggerCreated.disabled(), vNode);
        triggerStateStore.save(newState);
    }
}
