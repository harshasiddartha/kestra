package io.kestra.executor;

import io.kestra.core.models.flows.sla.SLAMonitor;

import java.time.Instant;
import java.util.function.Consumer;

public interface SLAMonitorStateStore {
    void save(SLAMonitor slaMonitor);

    void purge(String executionId);

    void processExpired(Instant now, Consumer<SLAMonitor> consumer);
}
