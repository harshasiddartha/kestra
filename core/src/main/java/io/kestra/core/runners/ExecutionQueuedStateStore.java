package io.kestra.core.runners;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.runners.ExecutionQueued;
import io.kestra.core.runners.TransactionContext;

import java.util.function.BiConsumer;

public interface ExecutionQueuedStateStore {
    void remove(Execution execution);

    void save(TransactionContext txContext, ExecutionQueued executionQueued);

    void pop(String tenantId, String namespace, String flowId, BiConsumer<TransactionContext, Execution> consumer);
}
