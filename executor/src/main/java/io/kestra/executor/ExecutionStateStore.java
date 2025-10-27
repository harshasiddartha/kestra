package io.kestra.executor;

import io.kestra.core.models.executions.Execution;

import java.util.function.Function;

public interface ExecutionStateStore {
    ExecutorContext lock(String executionId, Function<Execution, ExecutorContext> function);
}
