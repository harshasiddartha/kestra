package io.kestra.scheduler.models;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.FlowId;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.Backfill;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerId;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * Immutable class representing the state of a trigger.
 */
@Getter
@EqualsAndHashCode
@AllArgsConstructor
public final class TriggerState implements TriggerId {
    private final String tenantId;
    private final String namespace;
    private final String flowId;
    private final String triggerId;
    private final Instant updatedAt;
    private final ZonedDateTime evaluatedAt;
    private final ZonedDateTime nextEvaluationDate;
    private final TriggerStatus status;
    private final String executionId;
    private final Backfill backfill;
    private final List<State.Type> stopAfter;
    private final Boolean disabled;
    private final Long seqNo;
    private final Integer vnode;
    
    public TriggerContext context() {
        return TriggerContext.builder()
            .tenantId(tenantId)
            .namespace(namespace)
            .flowId(flowId)
            .triggerId(triggerId)
            .date(evaluatedAt)
            .stopAfter(stopAfter)
            .disabled(disabled)
            .nextExecutionDate(nextEvaluationDate)
            .build();
    }
    
    /**
     * Factory method for constructing a new {@link TriggerState}.
     *
     * @return a new {@link TriggerState}
     */
    public static TriggerState of(FlowId flowId, AbstractTrigger trigger, Integer vnode) {
        return of(TriggerId.of(flowId, trigger), trigger.getStopAfter(), trigger.isDisabled(), vnode);
    }
    
    /**
     * Factory method for constructing a new {@link TriggerState}.
     *
     * @return a new {@link TriggerState}
     */
    public static TriggerState of(TriggerId id, List<State.Type> stopAfter, Boolean disabled, Integer vnode) {
        return new TriggerState(
            id.getTenantId(),
            id.getNamespace(),
            id.getFlowId(),
            id.getTriggerId(),
            Instant.now(),
            null,
            null,
            TriggerStatus.IDLE,
            null,
            null,
            stopAfter,
            disabled,
            0L,
            vnode
        );
    }
    
    /**
     * Updates the status of this trigger state.
     *
     * @param clock the scheduler clock.
     * @return a new {@link TriggerState}
     */
    public TriggerState status(final Clock clock, final TriggerStatus status) {
        return new TriggerState(
            tenantId,
            namespace,
            flowId,
            triggerId,
            clock.instant(),
            evaluatedAt,
            nextEvaluationDate,
            status,
            executionId,
            backfill,
            stopAfter,
            disabled,
            seqNo,
            vnode
        );
    }
    
    /**
     * Updates the vNode of this trigger state.
     *
     * @param clock the scheduler clock.
     * @return a new {@link TriggerState}
     */
    public TriggerState vNode(final Clock clock, final int vnode) {
        return new TriggerState(
            tenantId,
            namespace,
            flowId,
            triggerId,
            clock.instant(),
            evaluatedAt,
            nextEvaluationDate,
            status,
            executionId,
            backfill,
            stopAfter,
            disabled,
            seqNo,
            vnode
        );
    }
    
    /**
     * Updates the evaluatedAt of this trigger state.
     *
     * @param clock the scheduler clock.
     * @return a new {@link TriggerState}
     */
    public TriggerState evaluatedAt(final Clock clock, final ZonedDateTime evaluatedAt) {
        return new TriggerState(
            tenantId,
            namespace,
            flowId,
            triggerId,
            clock.instant(),
            evaluatedAt,
            nextEvaluationDate,
            status,
            executionId,
            backfill,
            stopAfter,
            disabled,
            seqNo,
            vnode
        );
    }
    
    /**
     * Updates the state of the trigger for the given  {@code nextEvaluationDate}.
     *
     * @param clock              the scheduler clock.
     * @param nextEvaluationDate the next evaluation date.
     * @return a new {@link TriggerState}
     */
    public TriggerState updateForNextEvaluationDate(final Clock clock, final ZonedDateTime nextEvaluationDate) {
        return new TriggerState(
            tenantId,
            namespace,
            flowId,
            triggerId,
            clock.instant(),
            evaluatedAt,
            nextEvaluationDate,
            status,
            executionId,
            getBackFillForNextEvaluationDate(nextEvaluationDate),
            stopAfter,
            disabled,
            seqNo,
            vnode
        );
    }
    
    /**
     * Updates the state of the trigger for the given {@link Execution}.
     *
     * @param clock     the scheduler clock.
     * @param execution the execution.
     * @return a new {@link TriggerState}
     */
    public TriggerState updateForExecution(final Clock clock, final Execution execution) {
        return updateForExecutionState(clock, execution.getId(), execution.getState().getCurrent());
    }
    
    /**
     * Updates the state of the trigger for the given execution state.
     *
     * @param clock the scheduler clock.
     * @param state the execution state.
     * @return a new {@link TriggerState}
     */
    public TriggerState updateForExecutionState(final Clock clock, final State.Type state) {
        return updateForExecutionState(clock, executionId, state);
    }
    
    private TriggerState updateForExecutionState(final Clock clock, final String executionId, final State.Type state) {
        // switch disabled automatically if the executionEndState is one of the stopAfter states
        Boolean disabled = getStopAfter() != null ? getStopAfter().contains(state) : getDisabled();
        
        return new TriggerState(
            tenantId,
            namespace,
            flowId,
            triggerId,
            clock.instant(),
            evaluatedAt,
            nextEvaluationDate,
            status,
            state.isTerminated() ? null : executionId,
            backfill,
            stopAfter,
            disabled,
            seqNo,
            vnode
        );
    }
    
    private Backfill getBackFillForNextEvaluationDate(final ZonedDateTime nextEvaluationData) {
        if (backfill != null && !backfill.getPaused()) {
            if (nextEvaluationData.isAfter(backfill.getEnd())) {
                return null;
            } else {
                return backfill.toBuilder().currentDate(nextEvaluationData).build();
            }
        }
        return backfill;
    }
}
