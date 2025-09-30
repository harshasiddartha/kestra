package io.kestra.scheduler.events;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.kestra.core.models.Label;
import io.kestra.core.models.triggers.TriggerId;
import io.kestra.core.serializers.ListOrMapOfLabelDeserializer;
import io.kestra.core.serializers.ListOrMapOfLabelSerializer;
import io.kestra.core.validations.NoSystemLabelValidation;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

/**
 * A command to backfill a trigger.
 */
public record BackfillTrigger(
    TriggerId id,
    Instant timestamp,
    Backfill backfill
) implements TriggerEvent {
    
    public record Backfill(
        ZonedDateTime start,
        ZonedDateTime end,
        ZonedDateTime currentDate,
        Map<String, Object> inputs,
        @JsonSerialize(using = ListOrMapOfLabelSerializer.class)
        @JsonDeserialize(using = ListOrMapOfLabelDeserializer.class)
        List<@NoSystemLabelValidation Label> labels,
        ZonedDateTime previousNextExecutionDate
    ) {
    }
}
