package io.kestra.core.runners;

import java.util.function.BiConsumer;

/**
 * State store containing all workers' jobs in RUNNING state.
 *
 * @see WorkerJob
 */
public interface WorkerJobRunningStateStore {

    /**
     * Deletes a running worker job for the given key.
     *
     * <p>
     *     A key can be a {@link WorkerTask} Task Run ID.
     * </p>
     *
     * @param key the key of the worker job to be deleted.
     */
    void deleteByKey(String key);

    /**
     * Deletes a running worker job for the given key.
     *
     * <p>
     *     A key can be a {@link WorkerTask} Task Run ID.
     * </p>
     *
     * @param key the key of the worker job to be deleted.
     */
    void deleteByKey(TransactionContext txContext, String key);

    WorkerJobRunning save(TransactionContext txContext, WorkerJobRunning workerJobRunning);

    void processWorkerJobsForDeadWorkers(TransactionContext txContext, String workersUid, BiConsumer<TransactionContext, WorkerJobRunning> consumer);
}
