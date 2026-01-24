-- create table order events

CREATE TABLE order_events (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    schema_version INT NOT NULL,
    event_type TEXT NOT NULL,
    items JSONB,
    amount NUMERIC(10,2),
    event_ts BIGINT NOT NULL,
    event_time TIMESTAMPTZ
        GENERATED ALWAYS AS (
            to_timestamp(event_ts / 1000.0)
        ) STORED,
    kafka_topic TEXT NOT NULL,
    ingestion_ts TIMESTAMPTZ DEFAULT now()
);
