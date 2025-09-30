package io.kestra.scheduler;

import io.kestra.core.models.executions.Execution;

import java.util.Optional;

@Deprecated(forRemoval = true)
public interface SchedulerExecutionStateInterface {
    Optional<Execution> findById(String tenantId, String id);
}
