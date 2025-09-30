package io.kestra.scheduler;

import com.google.common.base.Throwables;
import io.kestra.core.exceptions.InternalException;
import io.kestra.core.metrics.MetricRegistry;
import io.kestra.core.models.conditions.Condition;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.FlowInterface;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.RecoverMissedSchedules;
import io.kestra.core.models.triggers.Schedulable;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerId;
import io.kestra.core.models.triggers.WorkerTriggerInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.services.ConditionService;
import io.kestra.core.services.LabelService;
import io.kestra.core.services.LogService;
import io.kestra.core.services.PluginDefaultService;
import io.kestra.core.utils.IdUtils;
import io.kestra.scheduler.internals.NextEvaluationDate;
import io.kestra.scheduler.internals.VNodes;
import io.kestra.scheduler.models.TriggerStatus;
import io.kestra.scheduler.internals.SchedulableEvaluator;
import io.kestra.scheduler.pubsub.TriggerExecutionPublisher;
import io.kestra.scheduler.pubsub.TriggerWorkerJobPublisher;
import io.kestra.scheduler.models.TriggerEvaluationContext;
import io.kestra.scheduler.models.TriggerState;
import io.kestra.scheduler.stores.FlowStateStore;
import io.kestra.scheduler.stores.TriggerStateStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 */
@Singleton
public class TriggerScheduler {
    
    private static final Logger log = LoggerFactory.getLogger(TriggerScheduler.class);
    
    // Config
    private final SchedulerConfiguration schedulerConfiguration;
    
    // Services
    private final RunContextFactory runContextFactory;
    private final LogService logService;
    private final ConditionService conditionService;
    private final TriggerWorkerJobPublisher triggerWorkerJobPublisher;
    private final SchedulableEvaluator schedulableEvaluator;
    private final TriggerExecutionPublisher triggerExecutionSender;
    private final PluginDefaultService pluginDefaultService;
    
    // Stores
    private final TriggerStateStore triggerStateStore;
    private final FlowStateStore flowStateStore;
    
    // Metrics
    private final MetricRegistry metricRegistry;
    private final Counter metricScheduleLoopCounter;
    private final Counter metricEvaluatedTriggerCounter;
    private final Timer metricEvaluationLoopDuration;
    
    public TriggerScheduler(TriggerStateStore triggerStateStore,
                            FlowStateStore flowStateStore,
                            MetricRegistry metricRegistry,
                            RunContextFactory runContextFactory,
                            ConditionService conditionService,
                            PluginDefaultService pluginDefaultService,
                            TriggerWorkerJobPublisher triggerWorkerJobPublisher,
                            SchedulableEvaluator schedulableEvaluator,
                            LogService logService,
                            TriggerExecutionPublisher triggerExecutionSender,
                            SchedulerConfiguration schedulerConfiguration) {
        this.triggerStateStore = triggerStateStore;
        this.flowStateStore = flowStateStore;
        this.runContextFactory = runContextFactory;
        this.pluginDefaultService = pluginDefaultService;
        this.logService = logService;
        this.metricRegistry = metricRegistry;
        this.conditionService = conditionService;
        this.triggerWorkerJobPublisher = triggerWorkerJobPublisher;
        this.schedulableEvaluator = schedulableEvaluator;
        this.triggerExecutionSender = triggerExecutionSender;
        this.schedulerConfiguration = schedulerConfiguration;
        // Metrics
        metricScheduleLoopCounter = metricRegistry
            .counter(MetricRegistry.METRIC_SCHEDULER_LOOP_COUNT, MetricRegistry.METRIC_SCHEDULER_LOOP_COUNT_DESCRIPTION);
        
        metricEvaluatedTriggerCounter = metricRegistry
            .counter(MetricRegistry.METRIC_SCHEDULER_EVALUATE_COUNT, MetricRegistry.METRIC_SCHEDULER_EVALUATE_COUNT_DESCRIPTION);
        
        metricEvaluationLoopDuration = metricRegistry
            .timer(MetricRegistry.METRIC_SCHEDULER_EVALUATION_LOOP_DURATION, MetricRegistry.METRIC_SCHEDULER_EVALUATION_LOOP_DURATION_DESCRIPTION);
    }
    
    public void onStart(final Clock clock, final Instant scheduledTime, final Set<Integer> vNodesAssignments) {
        log.info("Starting trigger scheduler on {} nodes", vNodesAssignments);
        
        Map<String, TriggerState> triggers = triggerStateStore.findForVNodes(vNodesAssignments)
            .stream().collect(Collectors.toMap(TriggerId::uid, Function.identity()));
        
        flowStateStore.findAllFlowsForVNodes(vNodesAssignments)
            .stream()
            .map(flow -> pluginDefaultService.injectAllDefaults(flow, log))
            .filter(Objects::nonNull)
            .filter(flow -> flow.getTriggers() != null && !flow.getTriggers().isEmpty())
            .flatMap(flow -> flow.getTriggers().stream().filter(trigger -> trigger instanceof WorkerTriggerInterface).map(trigger -> Pair.of(flow, trigger)))
            .distinct()
            .forEach(flowAndTrigger -> {
                final FlowWithSource flow = flowAndTrigger.getLeft();
                final AbstractTrigger trigger = flowAndTrigger.getRight();
                
                // Compute trigger vNode
                int vNode = VNodes.computeVNodeFromFlow(flow, schedulerConfiguration.vnodes());
                    
                // Check whether a state already exist for this trigger
                TriggerState triggerState = triggers.get(Trigger.uid(flow, trigger));

                if (triggerState == null) {
                    RunContext runContext = runContextFactory.of(flow, trigger);
                    ConditionContext conditionContext = conditionService.conditionContext(runContext, flow, null);
                    try {

                        // Create a TriggerState
                        TriggerState newTriggerState = TriggerState.of(flow, trigger, vNode);
                        
                        // new worker triggers will be evaluated immediately except schedule that will be evaluated at the next cron schedule
                        if (trigger instanceof WorkerTriggerInterface) {
                            newTriggerState = newTriggerState.updateForNextEvaluationDate(clock, ZonedDateTime.now(clock));
                        }
                        
                        if (trigger instanceof Schedulable schedulableTrigger) {
                            ZonedDateTime nextEvaluationDate = schedulableTrigger.nextEvaluationDate(conditionContext, Optional.empty());
                            newTriggerState = newTriggerState.updateForNextEvaluationDate(clock, nextEvaluationDate);
                        }
                        triggerStateStore.save(newTriggerState);
                        logService.logTrigger(newTriggerState, log, Level.INFO, "New state initialized");
                        
                    } catch (Exception e) {
                        logError(clock, conditionContext, flow, trigger, e);
                    }
                } else if (trigger instanceof Schedulable schedulableTrigger) {
                    // we recompute the Schedule nextExecutionDate if needed
                    RunContext runContext = runContextFactory.of(flow, trigger);
                    
                    ConditionContext conditionContext = conditionService.conditionContext(runContext, flow, null);
                    RecoverMissedSchedules recoverMissedSchedules = Optional.ofNullable(schedulableTrigger.getRecoverMissedSchedules()).orElseGet(() -> schedulableTrigger.defaultRecoverMissedSchedules(runContext));
                    try {
                        TriggerState currentTriggerState = triggerState;
                        boolean saved = false;
                        switch (recoverMissedSchedules) {
                            case LAST -> {
                                ZonedDateTime previousDate = schedulableTrigger.previousEvaluationDate(conditionContext);
                                if (previousDate.isAfter(currentTriggerState.getEvaluatedAt())) {
                                    currentTriggerState = currentTriggerState.updateForNextEvaluationDate(clock, previousDate);
                                    triggerStateStore.save(currentTriggerState.vNode(clock, vNode));
                                    saved = true;
                                }
                            }
                            case NONE -> {
                                ZonedDateTime nextEvaluationDate = schedulableTrigger.nextEvaluationDate();
                                if (!Objects.equals(currentTriggerState.getNextEvaluationDate(), nextEvaluationDate)) {
                                    currentTriggerState = currentTriggerState.updateForNextEvaluationDate(clock, nextEvaluationDate);
                                    triggerStateStore.save(currentTriggerState.vNode(clock, vNode));
                                    saved = true;
                                }
                            }
                            case ALL -> {
                                // nothing to do
                            }
                        }
                        
                        if (!saved && triggerState.getVnode() == null) {
                            triggerStateStore.save(triggerState.vNode(clock, vNode));
                        }
                    } catch (Exception e) {
                        logError(clock, conditionContext, flow, trigger, e);
                    }
                } else if (triggerState.getVnode() == null){
                    triggerStateStore.save(triggerState.vNode(clock, vNode));
                }
            });
    }
    
    public void onSchedule(final Clock clock, final Instant scheduledTime, final Set<Integer> vNodesAssignments) {
        metricScheduleLoopCounter.increment();
        
        ZonedDateTime zoneScheduleTime = ZonedDateTime.ofInstant(scheduledTime, clock.getZone());
        
        // Compute schedulable triggers only for this loop’s assignments
        List<TriggerEvaluationContext> schedulableTriggers = getSchedulableTriggers(clock, zoneScheduleTime, vNodesAssignments);
        
        if (log.isTraceEnabled()) {
            log.trace("Found {} schedulable triggers at {}", schedulableTriggers.size(), scheduledTime);
        }
        
        metricEvaluatedTriggerCounter.increment(schedulableTriggers.size());
        
        // Process Triggers
        schedulableTriggers.forEach(triggerEvaluationContext -> process(clock, zoneScheduleTime, triggerEvaluationContext));
        
        // Record metrics
        metricEvaluationLoopDuration.record(Duration.between(scheduledTime, clock.instant()));
    }
    
    private void process(Clock clock, ZonedDateTime scheduledTime, TriggerEvaluationContext triggerEvaluationContext) {
        
        final ConditionContext conditionContext = triggerEvaluationContext.conditionContext();
        final AbstractTrigger trigger = triggerEvaluationContext.trigger();
        final FlowInterface flow = triggerEvaluationContext.flow();
        final Logger logger = conditionContext.getRunContext().logger();
        
        TriggerState triggerState = triggerEvaluationContext.triggerState()
            .evaluatedAt(clock, scheduledTime);
            
        Execution execution = null;
        final TriggerContext triggerContext = triggerState.context();
        try {
            // conditionService.areValid can fail, so we cannot execute it early as we need to try/catch and send a failed executions
            List<Condition> conditions = trigger.getConditions() != null ? trigger.getConditions() : List.of();
            
            if (!conditionService.areValid(conditions, conditionContext)) {
                ZonedDateTime nextEvaluationDate = getNextEvaluationDateOrNullOnError(clock, triggerEvaluationContext);
                triggerState = triggerState.updateForNextEvaluationDate(clock, nextEvaluationDate);
                
            } else if (trigger instanceof Schedulable schedule) {
                Optional<Execution> maybeExecution = schedulableEvaluator.evaluate(schedule, triggerContext, triggerEvaluationContext.conditionContext());
                if (maybeExecution.isPresent()) {
                    log(clock, triggerContext, maybeExecution.get());
                    
                    ZonedDateTime nextEvaluationDate = schedule.nextEvaluationDate(conditionContext, Optional.of(triggerContext));
                    triggerState = triggerState
                        .updateForNextEvaluationDate(clock, nextEvaluationDate)
                        .updateForExecution(clock, maybeExecution.get())
                        .status(clock, TriggerStatus.EXECUTING);
                    execution = maybeExecution.get();
                    
                } else {
                    ZonedDateTime nextEvaluationDate = schedule.nextEvaluationDate(conditionContext, Optional.of(triggerContext));
                    triggerState = triggerState
                        .updateForNextEvaluationDate(clock, nextEvaluationDate);
                }
            } else if (trigger instanceof PollingTriggerInterface pollingTrigger && pollingTrigger.getInterval() == null) {
                logService.logTrigger(
                    triggerContext,
                    logger,
                    Level.ERROR,
                    "Worker trigger must have an interval (except the Schedule and Streaming)"
                );
            } else {
                triggerState = triggerState
                    .updateForNextEvaluationDate(clock, scheduledTime);
                try {
                    this.triggerWorkerJobPublisher.send(triggerEvaluationContext);
                    triggerState = triggerState.status(clock, TriggerStatus.POLLING);
                } catch (InternalException e) {
                    logService.logTrigger(
                        triggerContext,
                        logger,
                        Level.ERROR,
                        "Unable to send worker trigger to worker",
                        e
                    );
                }
            }
        } catch (Exception ie) {
            // validate schedule condition can fail to render variables
            // in this case, we send a failed execution so the trigger is not evaluated each second.
            logger.error("Unable to evaluate the trigger '{}'", trigger.getId(), ie);
            
            triggerState = triggerState
                .updateForNextEvaluationDate(clock, NextEvaluationDate.get(clock, trigger))
                .updateForExecutionState(clock, State.Type.FAILED)
                .status(clock, TriggerStatus.IDLE);
            
            execution = Execution.builder()
                .id(IdUtils.create())
                .tenantId(triggerContext.getTenantId())
                .namespace(triggerContext.getNamespace())
                .flowId(triggerContext.getFlowId())
                .flowRevision(flow.getRevision())
                .labels(LabelService.labelsExcludingSystem(flow))
                .state(new State().withState(State.Type.FAILED))
                .build();
        }
        
        triggerStateStore.save(triggerState);
        if (execution != null) {
            execution = execution
                .withScheduleDate(scheduledTime.toInstant())
                .withTenantId(triggerState.getTenantId());
            triggerExecutionSender.sendExecution(execution);
        }
    }
    
    private ZonedDateTime getNextEvaluationDateOrNullOnError(Clock clock, TriggerEvaluationContext triggerEvaluationContext) {
        Logger logger = triggerEvaluationContext.conditionContext().getRunContext().logger();
        ZonedDateTime nextEvaluationDate = null;
        try {
            nextEvaluationDate = NextEvaluationDate.get(clock, triggerEvaluationContext);
        } catch (Exception e) {
            logService.logTrigger(
                triggerEvaluationContext.triggerState(),
                logger,
                Level.WARN,
                "[date: {}] Evaluation failed. Error: '{}'",
                triggerEvaluationContext.triggerState().getEvaluatedAt(),
                e.getMessage(),
                e
            );
            
            if (logger.isTraceEnabled()) {
                logger.trace(Throwables.getStackTraceAsString(e));
            }
        }
        return nextEvaluationDate;
    }
    
    public List<TriggerEvaluationContext> getSchedulableTriggers(final Clock clock, final ZonedDateTime now, final Set<Integer> assignments) {
        List<TriggerState> triggers = this.triggerStateStore.findByNextExecutionDateReadyForAllTenants(now, assignments);
        
        return triggers.stream()
            .filter(triggerState -> !triggerState.getDisabled())
            .map(triggerState -> {
                Optional<FlowWithSource> maybeFlowTrigger = flowStateStore.findFlow(
                    triggerState.getTenantId(),
                    triggerState.getNamespace(),
                    triggerState.getFlowId()
                );
                
                // Check whether the Flow still exists
                if (maybeFlowTrigger.isEmpty()) {
                    triggerStateStore.delete(triggerState); // Delete triggerState state
                    return null;
                }
                
                final FlowWithSource flow = maybeFlowTrigger.get();
                
                // Validate that the trigger still exists and is enabled before processing. This check covers several cases:
                // 1. The overall Flow might be disabled 
                // 2. The specific trigger may have been removed.
                // 3. The trigger itself may have been disabled .
                // 
                // 2. and 3. can occur if the Flow has been updated but the associated TriggerEvent
                // has not yet been processed. In these cases, 
                final String triggerId = triggerState.getTriggerId();
                Optional<AbstractTrigger> maybeTrigger = flow.getTriggers().stream().filter(it -> it.getId().equals(triggerId)).findFirst();
                if (flow.isDisabled() || maybeTrigger.isEmpty() || maybeTrigger.get().isDisabled()) {
                    // Skip processing this trigger to avoid acting on stale or invalid trigger.
                    return null;
                }
                
                // TODO: Inject plugin default values for triggers
                AbstractTrigger trigger = maybeTrigger.get();
                
                RunContext runContext = runContextFactory.of(flow, trigger);
                ConditionContext conditionContext = ConditionContext.builder().flow(flow).runContext(runContext).build();
                
                if (triggerState.getNextEvaluationDate() == null) {
                    try {
                        ZonedDateTime nextEvaluationDate = NextEvaluationDate.get(clock, trigger, triggerState.context(), conditionContext);
                        triggerState = triggerState.updateForNextEvaluationDate(clock, nextEvaluationDate);
                    } catch (Exception e) {
                        logError(now, conditionContext, flow, trigger, e);
                        return null;
                    }
                }
                return new TriggerEvaluationContext(
                    flow,
                    trigger,
                    triggerState,
                    conditionContext.withVariables(
                        Map.of("trigger", Map.of("date", triggerState.getNextEvaluationDate() != null ?
                            triggerState.getNextEvaluationDate() : now.truncatedTo(ChronoUnit.SECONDS)))
                    )
                );
            })
            .filter(Objects::nonNull)
            .toList();
    }
    
    private void logError(final ZonedDateTime now, ConditionContext conditionContext, FlowInterface flow, AbstractTrigger trigger, Throwable e) {
        Logger logger = conditionContext.getRunContext().logger();
        
        logService.logExecution(
            flow,
            logger,
            Level.ERROR,
            "[trigger: {}] [date: {}] Evaluate Failed with error '{}'",
            trigger.getId(),
            now.truncatedTo(ChronoUnit.SECONDS),
            e.getMessage(),
            e
        );
    }
    
    private void log(Clock clock, TriggerContext triggerContext, Execution execution) {
        metricRegistry
            .counter(MetricRegistry.METRIC_SCHEDULER_TRIGGER_COUNT, MetricRegistry.METRIC_SCHEDULER_TRIGGER_COUNT_DESCRIPTION, metricRegistry.tags(execution))
            .increment();
        
        ZonedDateTime now = ZonedDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
        
        if (execution.getTrigger() != null &&
            execution.getTrigger().getVariables() != null &&
            execution.getTrigger().getVariables().containsKey("next")
        ) {
            Object nextVariable = execution.getTrigger().getVariables().get("next");
            
            ZonedDateTime next = (nextVariable != null) ? ZonedDateTime.parse((CharSequence) nextVariable) : null;
            
            // Exclude backfills
            // FIXME : "late" are not excluded and can increase delay value (false positive)
            if (next != null && now.isBefore(next)) {
                metricRegistry
                    .timer(MetricRegistry.METRIC_SCHEDULER_TRIGGER_DELAY_DURATION, MetricRegistry.METRIC_SCHEDULER_TRIGGER_DELAY_DURATION_DESCRIPTION, metricRegistry.tags(execution))
                    .record(Duration.between(triggerContext.getDate(), now));
            }
        }
        
        logService.logTrigger(
            triggerContext,
            Level.INFO,
            "Scheduled execution {} at '{}' started at '{}'",
            execution.getId(),
            triggerContext.getDate(),
            now
        );
    }
    
    private void logError(Clock clock, ConditionContext conditionContext, FlowWithSource flow, AbstractTrigger
        trigger, Throwable e) {
        Logger logger = conditionContext.getRunContext().logger();
        
        logService.logExecution(
            flow,
            logger,
            Level.ERROR,
            "[trigger: {}] [date: {}] Evaluate Failed with error '{}'",
            trigger.getId(),
            clock,
            e.getMessage(),
            e
        );
    }
}
