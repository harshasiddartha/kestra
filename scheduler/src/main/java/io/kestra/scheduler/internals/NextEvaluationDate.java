package io.kestra.scheduler.internals;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.scheduler.models.TriggerEvaluationContext;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Optional;

public class NextEvaluationDate {
    
    public static ZonedDateTime get(Clock clock, TriggerEvaluationContext triggerEvaluationContext) throws Exception {
        return get(clock, triggerEvaluationContext.trigger(), triggerEvaluationContext.triggerState().context(), triggerEvaluationContext.conditionContext());
    }
    
    public static ZonedDateTime get(Clock clock, AbstractTrigger trigger, TriggerContext triggerContext, ConditionContext conditionContext) throws Exception {
        ZonedDateTime nextExecutionDate;
        
        if (trigger instanceof PollingTriggerInterface pollingTrigger) {
            nextExecutionDate = pollingTrigger.nextEvaluationDate(conditionContext, Optional.ofNullable(triggerContext));
        } else {
            nextExecutionDate = ZonedDateTime.now(clock); // real-time trigger
        }
        
        return nextExecutionDate;
    }
    
    public static ZonedDateTime get(Clock clock, AbstractTrigger trigger) {
        ZonedDateTime nextExecutionDate;
        if (trigger instanceof PollingTriggerInterface pollingTrigger) {
            nextExecutionDate = pollingTrigger.nextEvaluationDate();
        } else {
            nextExecutionDate = ZonedDateTime.now(clock); // real-time trigger
        }
        
        return nextExecutionDate;
    }
}
