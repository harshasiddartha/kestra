package io.kestra.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.DeserializationException;
import io.kestra.core.utils.Disposable;
import io.kestra.core.utils.Either;
import io.kestra.core.utils.ExecutorsUtils;
import io.kestra.jdbc.JdbcMapper;
import io.kestra.jdbc.JooqDSLContextWrapper;
import io.kestra.jdbc.runner.JdbcQueueConfiguration;
import io.kestra.jdbc.runner.JdbcQueuePoller;
import io.kestra.jdbc.runner.JdbcRunnerEnabled;
import io.kestra.scheduler.events.TriggerEvent;
import io.kestra.scheduler.internals.VNodes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Singleton
@JdbcRunnerEnabled
public class DefaultJdbcTriggerEventQueue implements TriggerEventQueue {
    
    private static final Logger log = LoggerFactory.getLogger(DefaultJdbcTriggerEventQueue.class);
    
    private static final ObjectMapper MAPPER = JdbcMapper.of();
    
    // Tables
    private static final String QUEUE_TABLE_NAME = "queue_trigger_event";
    
    // Columns
    private static final Field<Object> KEY_FIELD = DSL.field(DSL.quotedName("key"));
    private static final Field<Object> VALUE_FIELD = DSL.field(DSL.quotedName("value"));
    private static final Field<Object> VNODE_FIELD = DSL.field(DSL.quotedName("vnode"));
    private static final Field<Long> OFFSET_FIELD = DSL.field(DSL.quotedName("offset"), Long.class);
    
    private final JdbcQueueConfiguration jdbcQueueConfiguration;
    private final SchedulerConfiguration schedulerConfiguration;
    private final JooqDSLContextWrapper dslContextWrapper;
    private final Table<Record> table;
    
    private final ExecutorService executor;
    private final JdbcQueueOffsetManager offsetManager;
    
    private final ConcurrentHashMap<Subscription, JdbcQueuePoller> subscriptions = new ConcurrentHashMap<>();
    
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    @Inject
    public DefaultJdbcTriggerEventQueue(JooqDSLContextWrapper dslContextWrapper,
                                        JdbcQueueOffsetManager offsetManager,
                                        SchedulerConfiguration schedulerConfiguration,
                                        JdbcQueueConfiguration jdbcQueueConfiguration,
                                        ExecutorsUtils executorsUtils) {
        this.table = DSL.table(QUEUE_TABLE_NAME);
        this.dslContextWrapper = dslContextWrapper;
        this.schedulerConfiguration = schedulerConfiguration;
        this.jdbcQueueConfiguration = jdbcQueueConfiguration;
        this.executor = executorsUtils.cachedThreadPool("jdbc-queue-trigger-event");
        this.offsetManager = offsetManager;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void send(final TriggerEvent triggerEvent) {
        Objects.requireNonNull(triggerEvent, "triggerEvent must not be null");
        
        final JSONB value = mapToJSONB(triggerEvent);
        final int vNode = VNodes.computeVNodeFromTrigger(triggerEvent.id(), schedulerConfiguration.vnodes());
        dslContextWrapper.transaction(configuration -> {
            try {
                DSLContext ctx = DSL.using(configuration);
                int inserted = ctx.insertInto(table)
                    .set(KEY_FIELD, triggerEvent.uid())
                    .set(VALUE_FIELD, value)
                    .set(VNODE_FIELD, vNode)
                    .execute();
                log.info("Inserted {} row(s) into {}", inserted, table.getName());
            } catch (DataAccessException e) {
                e.printStackTrace();
            }
        });
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Disposable subscribe(Subscription subscription, BiConsumer<Integer, TriggerEvent> handler) {
        checkExclusiveSubscription(subscription);
        
        // fetch last consumed offset for each vNode
        final Map<Integer, Long> lastOffsetsForVNodes = subscription.vNodes().stream()
            .map(vNode -> Map.entry(vNode, offsetManager.fetchLastConsumedOffset(QUEUE_TABLE_NAME, subscription.name(), vNode)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        
        // The callable executed by the poller
        Callable<Integer> pollTask = () -> {
            AtomicInteger consumed = new AtomicInteger();
            dslContextWrapper.transaction(configuration -> {
                DSLContext ctx = DSL.using(configuration);
                
                for (Integer vNode : subscription.vNodes()) {
                    // Fetch next unprocessed event
                    Result<Record> result = ctx.select(KEY_FIELD, VALUE_FIELD, VNODE_FIELD, OFFSET_FIELD)
                        .from(table)
                        .where(VNODE_FIELD.eq(vNode))
                        .and(OFFSET_FIELD.gt(lastOffsetsForVNodes.get(vNode)))
                        .orderBy(OFFSET_FIELD.asc())
                        .limit(jdbcQueueConfiguration.pollSize())
                        .forUpdate()
                        .skipLocked()
                        .fetchMany()
                        .getFirst();
                    
                    if (!result.isEmpty()) {
                        // Process events
                        mapToEntities(result, TriggerEvent.class)
                            .forEach(entity -> {
                                if (entity.isRight()) {
                                    log.warn("Cannot consume TriggerEvent. Cause: {}", entity.getRight().getMessage());
                                } else {
                                    handler.accept(vNode, entity.getLeft());
                                }
                            });
                        
                        // Commit last consumed offset
                        Long lastOffset = result.map(record -> record.get(OFFSET_FIELD)).getLast();
                        lastOffsetsForVNodes.put(vNode, lastOffset);
                        
                        offsetManager.commitLastConsumedOffset(ctx, QUEUE_TABLE_NAME, subscription.name(), vNode, lastOffset);
                    }
                    
                    consumed.addAndGet(result.size());
                }
            });
            return consumed.get();
        };
        
        JdbcQueuePoller poller = new JdbcQueuePoller(jdbcQueueConfiguration, pollTask);
        subscriptions.put(subscription, poller);
        executor.execute(poller);
        
        return Disposable.of(() -> 
            Optional.ofNullable(subscriptions.remove(subscription)).ifPresent(JdbcQueuePoller::stop)
        );
    }
    
    private void checkExclusiveSubscription(final Subscription subscription) {
        subscriptions.keySet().stream()
            .filter(existing -> existing.name().equals(subscription.name()))
            .forEach(existing -> {
                Set<Integer> intersection = new HashSet<>(existing.vNodes());
                intersection.retainAll(subscription.vNodes());
                if (!intersection.isEmpty()) {
                    Integer vNode = intersection.iterator().next();
                    throw new SubscriptionBusyException(QUEUE_TABLE_NAME, subscription.name(), vNode);
                }
            });
    }
    
    private static JSONB mapToJSONB(Object entity) {
        try {
            return JSONB.valueOf(new String(MAPPER.writeValueAsBytes(entity)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static <T> List<Either<T, DeserializationException>> mapToEntities(Result<Record> fetch, Class<T> type) {
        return fetch
            .map(record -> {
                try {
                    return Either.left(MAPPER.readValue(record.get("value", String.class), type));
                } catch (JsonProcessingException e) {
                    return Either.right(new DeserializationException(e, record.get("value", String.class)));
                }
            });
    }
    
    /** {@inheritDoc} **/
    @Override
    public void close() throws IOException {
        if (!this.closed.compareAndSet(true, false)) {
            return; // already stopped
        }
        
        subscriptions.values().forEach(JdbcQueuePoller::stop);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting for executor to shut down");
        }
    }
}
