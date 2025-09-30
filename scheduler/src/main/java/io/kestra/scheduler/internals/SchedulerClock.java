package io.kestra.scheduler.internals;

import java.time.Clock;
import java.time.ZonedDateTime;

/**
 * 
 */
public final class SchedulerClock {
    
    private static final SchedulerClock INSTANCE = new SchedulerClock();
    
    private Clock clock = Clock.systemDefaultZone();
    
    private SchedulerClock() {}
    
    public static Clock getClock() {
        return INSTANCE.clock;
    }
    
    public static void setClock(final Clock clock) {
        INSTANCE.clock = clock;
    }
    
    public static ZonedDateTime now() {
        return ZonedDateTime.now(INSTANCE.clock);
    }
}
