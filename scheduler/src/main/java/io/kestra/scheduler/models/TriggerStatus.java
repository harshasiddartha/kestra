package io.kestra.scheduler.models;

/**
 * Represents the current functional state of a trigger.
 * <p>
 * This is distinct from {@link TriggerState}, which contains the full metadata of a trigger.
 */
public enum TriggerStatus {
    /**
     * The trigger is idle, neither polling nor attached to a running execution.
     */
    IDLE,
    
    /**
     * The trigger is being evaluated by worker.
     * <p>
     * During this state, the trigger is locked and cannot be reevaluated.
     * <p>
     * This status is only for:
     * <ul>
     * <li>{@link io.kestra.core.models.triggers.PollingTriggerInterface}</li>
     * <li>{@link io.kestra.core.models.triggers.RealtimeTriggerInterface}</li>
     * </ul>
     */
    POLLING,
    
    /**
     * The trigger has produced a running execution.
     * <p>
     * During this state, the trigger is locked and cannot be reevaluated.
     * This status is only for:
     * <ul>
     * <li>{@link io.kestra.core.models.triggers.Schedulable}</li>
     * <li>{@link io.kestra.core.models.triggers.PollingTriggerInterface}</li>
     * </ul>
     */
    EXECUTING
}
