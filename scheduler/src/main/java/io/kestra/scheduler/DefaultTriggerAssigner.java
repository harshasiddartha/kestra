package io.kestra.scheduler;

import io.kestra.core.repositories.ServiceInstanceRepositoryInterface;
import io.kestra.core.server.Service;
import io.kestra.core.server.ServiceInstance;
import io.kestra.core.server.ServiceType;
import io.kestra.scheduler.internals.VNodes;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link TriggerAssigner} that assigns trigger virtual nodes (vNodes)
 * to active scheduler services.
 * <p>
 * This assigner periodically polls the {@link ServiceInstanceRepositoryInterface} to detect
 * active schedulers and compute vNode ownership. It notifies registered {@link TriggerAssignmentListener}
 * instances whenever assignments change.
 *
 * <p>Threading:
 * <ul>
 *     <li>Listener callbacks are invoked asynchronously in a separate executor to prevent blocking the scheduler loop.</li>
 *     <li>The polling interval is defined by {@link #SCHEDULE_INTERVAL_SECONDS}.</li>
 * </ul>
 */
@Singleton
public class DefaultTriggerAssigner implements TriggerAssigner {
    
    private static final Logger LOG = LoggerFactory.getLogger(DefaultTriggerAssigner.class);
    
    private final int vNodeCount;
    private final Map<String, TriggerAssignmentListener> listeners = new ConcurrentHashMap<>();
    private final ServiceInstanceRepositoryInterface serviceInstanceRepository;
    
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService listenerExecutor = Executors.newCachedThreadPool();
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    
    private volatile Set<String> activeSchedulerServices = Set.of();
    
    private static final long SCHEDULE_INTERVAL_SECONDS = 30;
    
    /**
     * Creates a new DefaultTriggerAssigner with the default vNode count of 16.
     *
     * @param serviceInstanceRepository repository to fetch active scheduler instances
     */
    @Inject
    public DefaultTriggerAssigner(SchedulerConfiguration config, ServiceInstanceRepositoryInterface serviceInstanceRepository) {
        this(config.vnodes(), serviceInstanceRepository);
    }
    
    /**
     * Creates a new DefaultTriggerAssigner with a custom vNode count.
     *
     * @param vNodeCount                number of virtual nodes for assignment (must be > 0)
     * @param serviceInstanceRepository repository to fetch active scheduler instances
     */
    public DefaultTriggerAssigner(int vNodeCount, ServiceInstanceRepositoryInterface serviceInstanceRepository) {
        if (vNodeCount <= 0) {
            throw new IllegalArgumentException("vNodeCount must be positive");
        }
        this.vNodeCount = vNodeCount;
        this.serviceInstanceRepository = Objects.requireNonNull(serviceInstanceRepository, "serviceInstanceRepository must not be null");
        startScheduler();
    }
    
    /**
     * Subscribes a {@link TriggerAssignmentListener} to receive notifications about trigger
     * vNode assignment changes for the given scheduler.
     *
     * @param schedulerId the unique identifier of the scheduler service
     * @param listener    the listener that will receive assignment notifications
     */
    @Override
    public void subscribe(String schedulerId, TriggerAssignmentListener listener) {
        Objects.requireNonNull(schedulerId, "schedulerId must not be null");
        Objects.requireNonNull(listener, "listener must not be null");
        
        listeners.put(schedulerId, new LoggerTriggerAssignmentListener(listener, LOG));
    }
    
    /**
     * Starts the internal scheduled executor for polling active schedulers.
     */
    private void startScheduler() {
        scheduler.scheduleAtFixedRate(this::pollSchedulers, 0, SCHEDULE_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Polls the service repository to detect active schedulers and notify listeners
     * about assignment changes.
     */
    private void pollSchedulers() {
        try {
            List<ServiceInstance> currentActiveSchedulers = serviceInstanceRepository.findAllInstancesInState(Service.ServiceState.RUNNING)
                .stream()
                .filter(service -> service.is(ServiceType.SCHEDULER))
                .toList();
            
            Set<String> currentActiveSchedulerIds = currentActiveSchedulers.stream()
                .map(ServiceInstance::uid)
                .collect(Collectors.toSet());
            
            if (!currentActiveSchedulerIds.equals(activeSchedulerServices)) {
                // Step 1: Notify revocation asynchronously
                List<CompletableFuture<Void>> revocationTasks = listeners.values().stream()
                    .map(listener -> CompletableFuture.runAsync(
                        listener::onTriggerAssignmentRevoked, listenerExecutor))
                    .toList();

                // Step 2: After all revocations complete, run assignment
                CompletableFuture.allOf(revocationTasks.toArray(new CompletableFuture[0]))
                    .thenRunAsync(() -> {
                        // Compute ordered service IDs
                        List<String> serviceIdsOrdered = currentActiveSchedulers.stream()
                            .sorted(Comparator.comparing(s -> s.createdAt().toEpochMilli()))
                            .map(ServiceInstance::uid)
                            .toList();
                        
                        // Notify assignment asynchronously
                        listeners.forEach((schedulerId, listener) -> {
                            if (currentActiveSchedulerIds.contains(schedulerId)) {
                                Set<Integer> assignedVNodes = VNodes.computeVNodeOwnership(schedulerId, serviceIdsOrdered, vNodeCount);
                                listenerExecutor.execute(() -> {
                                        try {
                                            listener.onTriggerAssignmentAssigned(assignedVNodes);
                                        } catch (Exception e) {
                                            LOG.error("Unexpected error while assigning vNodes", e);
                                        }
                                    }
                                );
                            }
                        });
                        
                        // Update active schedulers
                        activeSchedulerServices = currentActiveSchedulerIds;
                        
                    }, listenerExecutor)
                    
                    .exceptionally(e -> {
                        LOG.warn("Exception occurred while polling active schedulers.", e);
                        return null;
                    });
            }
        } catch (Exception e) {
            LOG.warn("Exception occurred while polling active schedulers.", e);
        }
    }
    
    /**
     * Stops the internal scheduler and terminates all pending tasks.
     * <p>
     * This method is idempotent and safe to call multiple times.
     */
    @PreDestroy
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return; // Already stopped
        }
        
        scheduler.shutdownNow();
        listenerExecutor.shutdownNow();
        
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
                System.err.println("Scheduler did not terminate within timeout");
            }
            if (!listenerExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                System.err.println("Listener executor did not terminate within timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
