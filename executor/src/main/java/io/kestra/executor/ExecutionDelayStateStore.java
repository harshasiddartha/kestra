package io.kestra.executor;

import io.kestra.core.runners.ExecutionDelay;

import java.util.function.Consumer;

public interface ExecutionDelayStateStore {
    void get(Consumer<ExecutionDelay> consumer);

    void save(ExecutionDelay executionDelay);
}
