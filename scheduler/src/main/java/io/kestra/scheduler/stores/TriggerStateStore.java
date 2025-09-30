package io.kestra.scheduler.stores;

import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.models.triggers.TriggerId;
import io.kestra.core.repositories.TriggerRepositoryInterface;
import io.kestra.scheduler.SchedulerConfiguration;
import io.kestra.scheduler.internals.VNodes;
import io.kestra.scheduler.models.TriggerState;
import jakarta.inject.Inject;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class TriggerStateStore {
    
    private final TriggerRepositoryInterface triggerRepository;
    private final SchedulerConfiguration schedulerConfiguration;
    
    @Inject
    public TriggerStateStore(SchedulerConfiguration schedulerConfiguration,
                             TriggerRepositoryInterface triggerRepository) {
        this.triggerRepository = triggerRepository;
        this.schedulerConfiguration = schedulerConfiguration;
    }
    
    public List<TriggerState> findByNextExecutionDateReadyForAllTenants(ZonedDateTime now, Set<Integer> vNodes) {
        return triggerRepository.findByNextExecutionDateReadyForAllTenants(now, vNodes)
            .stream()
            .map(TriggerStateAdapter::fromTrigger)
            .toList();
    }
    
    public List<TriggerState> findForVNodes(final Set<Integer> vNodes) {
        return this.triggerRepository.findAllForAllTenants()
            .stream()
            .filter(f -> vNodes.contains(VNodes.computeVNodeFromTrigger(TriggerId.of(f), schedulerConfiguration.vnodes())))
            .map(TriggerStateAdapter::fromTrigger)
            .toList();
    }
    
    public Optional<TriggerState> find(TriggerId triggerId) {
        return doFind(triggerId).map(TriggerStateAdapter::fromTrigger);
    }
    
    public void save(TriggerState triggerState) {
        Trigger entity = TriggerStateAdapter.toTrigger(triggerState);
        triggerRepository.save(entity);
    }
    
    public void delete(TriggerId triggerId) {
        doFind(triggerId).ifPresent(triggerRepository::delete);
    }
    
    private Optional<Trigger> doFind(TriggerId triggerId) {
        return triggerRepository.findLast(triggerId);
    }
    
    @VisibleForTesting
    static class TriggerStateAdapter{
        
        public static TriggerState fromTrigger(Trigger trigger) {
            return new TriggerState(
                trigger.getTenantId(),
                trigger.getNamespace(), 
                trigger.getFlowId(),
                trigger.getTriggerId(),
                trigger.getUpdatedDate(),
                trigger.getDate(),
                trigger.getNextExecutionDate(),
                null,
                trigger.getExecutionId(),
                trigger.getBackfill(),
                trigger.getStopAfter(),
                trigger.getDisabled(),
                0L, // TODO
                trigger.getVnode()
            );
        }
        
        public static Trigger toTrigger(TriggerState triggerState) {
            return Trigger.builder()
                .tenantId(triggerState.getTenantId())
                .namespace(triggerState.getNamespace())
                .flowId(triggerState.getFlowId())
                .triggerId(triggerState.getTriggerId())
                .date(triggerState.getEvaluatedAt())
                .backfill(triggerState.getBackfill())
                .stopAfter(triggerState.getStopAfter())
                .disabled(triggerState.getDisabled())
                .nextExecutionDate(triggerState.getNextEvaluationDate())
                .executionId(triggerState.getExecutionId())
                .vnode(triggerState.getVnode())
                .build();
        }
        
    }
}
