package io.kestra.scheduler;

import io.kestra.jdbc.JooqDSLContextWrapper;
import io.kestra.jdbc.runner.JdbcRunnerEnabled;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@JdbcRunnerEnabled
public class JdbcQueueOffsetManager {
    
    private static final Logger log = LoggerFactory.getLogger(JdbcQueueOffsetManager.class);
    
    // Table
    private static final String OFFSET_TABLE_NAME = "queue_consumer_offset";
    
    // Columns
    private static final Field<Long> OFFSET_FIELD = DSL.field(DSL.quotedName("offset"), Long.class);
    private static final Field<Object> SUBSCRIPTION_FIELD = DSL.field(DSL.quotedName("subscription"));
    private static final Field<Object> QUEUE_FIELD = DSL.field(DSL.quotedName("queue"));
    private static final Field<Integer> VNODE_FIELD = DSL.field(DSL.quotedName("vnode"), Integer.class);
    private final JooqDSLContextWrapper dslContextWrapper;
    private final Table<Record> table;
    
    @Inject
    public JdbcQueueOffsetManager(JooqDSLContextWrapper dslContextWrapper) {
        this.table = DSL.table(OFFSET_TABLE_NAME);
        this.dslContextWrapper = dslContextWrapper;
    }
    
    public long fetchLastConsumedOffset(String queue, String subscription, Integer vnode) {
        return dslContextWrapper.transactionResult(configuration -> {
            DSLContext ctx = DSL.using(configuration);
            Long offset = ctx
                .select(DSL.coalesce(OFFSET_FIELD, DSL.inline(0L)))
                .from(OFFSET_TABLE_NAME)
                .where(SUBSCRIPTION_FIELD.eq(subscription))
                .and(QUEUE_FIELD.eq(queue))
                .and(VNODE_FIELD.eq(vnode))
                .fetchOne(0, Long.class);
            return offset != null ? offset : 0L;
        });
    }
    
    public void commitLastConsumedOffset(DSLContext ctx, String queue, String subscription, Integer vnode, Long offset) {
        try {
            int execute = ctx.insertInto(table)
                .set(SUBSCRIPTION_FIELD, subscription)
                .set(QUEUE_FIELD, queue)
                .set(VNODE_FIELD, vnode)
                .set(OFFSET_FIELD, offset)
                .onConflict(SUBSCRIPTION_FIELD, QUEUE_FIELD, VNODE_FIELD)
                .doUpdate()
                .set(OFFSET_FIELD, offset)
                .execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
