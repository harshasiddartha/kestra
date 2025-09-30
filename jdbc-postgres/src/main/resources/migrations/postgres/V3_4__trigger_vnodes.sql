ALTER TABLE triggers add "vnode" INTEGER GENERATED ALWAYS AS ((value ->> 'vnode')::integer) STORED;
CREATE INDEX IF NOT EXISTS idx_triggers_vnode ON triggers (vnode);

ALTER TYPE queue_type ADD VALUE IF NOT EXISTS 'io.kestra.scheduler.events.TriggerEvent';

CREATE TABLE IF NOT EXISTS queue_trigger_event (
    "offset" BIGSERIAL PRIMARY KEY,
    key VARCHAR(250),
    value JSONB NOT NULL,
    vnode SMALLINT NOT NULL,
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_queue_trigger_event_vnode_offset
    ON queue_trigger_event (vnode, "offset");

CREATE TABLE queue_consumer_offset (
   subscription VARCHAR(250) NOT NULL,
   queue VARCHAR(250) NOT NULL,
   vnode SMALLINT,
   "offset" BIGINT NOT NULL,
   updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
   PRIMARY KEY (subscription, queue, vnode)
);