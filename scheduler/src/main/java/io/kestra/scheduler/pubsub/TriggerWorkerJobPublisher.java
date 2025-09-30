package io.kestra.scheduler.pubsub;

import io.kestra.core.exceptions.InternalException;
import io.kestra.core.models.tasks.WorkerGroup;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.queues.QueueException;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.WorkerGroupExecutorInterface;
import io.kestra.core.runners.WorkerJob;
import io.kestra.core.runners.WorkerTrigger;
import io.kestra.core.services.LogService;
import io.kestra.core.services.WorkerGroupService;
import io.kestra.scheduler.models.TriggerEvaluationContext;
import io.kestra.scheduler.models.TriggerState;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.Optional;

//TODO should not be part of the scheduler
@Singleton
public class TriggerWorkerJobPublisher {
    
    private static final Logger log = LoggerFactory.getLogger(TriggerWorkerJobPublisher.class);
    
    private final LogService logService;
    private final WorkerGroupExecutorInterface workerGroupExecutor;
    private final WorkerGroupService workerGroupService;
    private final QueueInterface<WorkerJob> workerJobQueue;
    
    @Inject
    public TriggerWorkerJobPublisher(LogService logService, WorkerGroupExecutorInterface workerGroupExecutor, WorkerGroupService workerGroupService, QueueInterface<WorkerJob> workerJobQueue) {
        this.logService = logService;
        this.workerGroupExecutor = workerGroupExecutor;
        this.workerGroupService = workerGroupService;
        this.workerJobQueue = workerJobQueue;
    }
    
    public void send(TriggerEvaluationContext triggerEvaluationContext) throws InternalException {
        
        TriggerState triggerState = triggerEvaluationContext.triggerState();
        
        if (log.isDebugEnabled()) {
            logService.logTrigger(
                triggerState,
                Level.DEBUG,
                "[date: {}] Scheduling evaluation to the worker",
                triggerState.getEvaluatedAt()
            );
        }
        
        // TODO - Do we really need to send all that data ???
        var workerTrigger = WorkerTrigger
            .builder()
            .trigger(triggerEvaluationContext.trigger())
            .triggerContext(
                // TODO - could be probably replace by the TriggerContext
                Trigger.builder()
                    .tenantId(triggerState.getTenantId())
                    .namespace(triggerState.getNamespace())
                    .flowId(triggerState.getFlowId())
                    .triggerId(triggerState.getTriggerId())
                    .date(triggerState.getEvaluatedAt())
                    .backfill(triggerState.getBackfill())
                    .stopAfter(triggerState.getStopAfter())
                    .disabled(triggerState.getDisabled())
                    .build()
            )
            .conditionContext(triggerEvaluationContext.conditionContext())
            .build();
        try {
            Optional<WorkerGroup> workerGroup = workerGroupService.resolveGroupFromJob(triggerEvaluationContext.flow(), workerTrigger);
            if (workerGroup.isPresent()) {
                // Check if the worker group exist
                String tenantId = triggerState.getTenantId();
                RunContext runContext = triggerEvaluationContext.conditionContext().getRunContext();
                String workerGroupKey = runContext.render(workerGroup.get().getKey());
                if (workerGroupExecutor.isWorkerGroupExistForKey(workerGroupKey, tenantId)) {
                    // Check whether at-least one worker is available
                    if (workerGroupExecutor.isWorkerGroupAvailableForKey(workerGroupKey)) {
                        this.workerJobQueue.emit(workerGroupKey, workerTrigger);
                    } else {
                        WorkerGroup.Fallback fallback = workerGroup.map(WorkerGroup::getFallback).orElse(WorkerGroup.Fallback.WAIT);
                        switch(fallback) {
                            case FAIL -> runContext.logger()
                                .error("No workers are available for worker group '{}', ignoring the trigger.", workerGroupKey);
                            case CANCEL -> runContext.logger()
                                .warn("No workers are available for worker group '{}', ignoring the trigger.", workerGroupKey);
                            case WAIT -> {
                                runContext.logger()
                                    .info("No workers are available for worker group '{}', waiting for one to be available.", workerGroupKey);
                                this.workerJobQueue.emit(workerGroupKey, workerTrigger);
                            }
                        };
                    }
                } else {
                    runContext.logger().error("No worker group exist for key '{}', ignoring the trigger.", workerGroupKey);
                }
            } else {
                this.workerJobQueue.emit(workerTrigger);
            }
        } catch (QueueException e) {
            log.error("Unable to emit the Worker Trigger job", e);
        }
    }
}
