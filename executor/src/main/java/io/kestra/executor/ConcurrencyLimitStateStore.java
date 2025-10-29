package io.kestra.executor;

import io.kestra.core.models.flows.FlowInterface;
import io.kestra.core.runners.ConcurrencyLimit;
import io.kestra.core.runners.ExecutionRunning;
import io.kestra.core.runners.TransactionContext;
import org.apache.commons.lang3.tuple.Pair;

import java.util.function.BiFunction;

public interface ConcurrencyLimitStateStore {
    ExecutionRunning countThenProcess(FlowInterface flow, BiFunction<TransactionContext, ConcurrencyLimit, Pair<ExecutionRunning, ConcurrencyLimit>> consumer);

    void decrement(FlowInterface flow);

    void increment(TransactionContext txContext, FlowInterface flow);
}
