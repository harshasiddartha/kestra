package io.kestra.scheduler.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.kestra.core.utils.Enums;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Supported event or command types for trigger.
 */
public enum Type {
    // EVENTS
    TRIGGER_CREATED,
    TRIGGER_UPDATED,
    TRIGGER_DELETED,
    TRIGGER_EXECUTED,
    TRIGGER_COMPLETED,
    // COMMANDS,
    BACKFILL_TRIGGER,
    RESET_TRIGGER,
    DISABLE_TRIGGER,
    // ERROR
    INVALID;
    
    private static final Pattern NORMALIZE = Pattern.compile("([a-z])([A-Z])");

    public static Type from(final Class<? extends TriggerEvent> event) {
        return from(NORMALIZE.matcher(event.getSimpleName()).replaceAll("$1_$2").toUpperCase(Locale.ROOT));
    }
    
    @JsonCreator
    public static Type from(final String s) {
        String normalized = s.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase();
        return Enums.getForNameIgnoreCase(normalized, Type.class, INVALID);
    }
}
