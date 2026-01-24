-- 1. Bronze: The landing zone
CREATE TABLE IF NOT EXISTS bronze.order_events (
    event_id UUID,
    order_id UUID,
    schema_version INT,
    event_type TEXT,
    items JSONB,
    amount NUMERIC(10,2),
    event_ts BIGINT,
    event_time TIMESTAMPTZ,
    kafka_topic TEXT,
    ingestion_ts TIMESTAMPTZ
);

-- Silver: Cleaned / enriched
CREATE TABLE silver.order_events (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    schema_version INT NOT NULL,
    event_type TEXT NOT NULL,
    items JSONB,
    amount NUMERIC(10,2),
    event_ts BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    kafka_topic TEXT NOT NULL,
    ingestion_ts TIMESTAMPTZ NOT NULL,
    processed_ts TIMESTAMPTZ DEFAULT now()
);

-- Gold: Aggregated & Business Ready
-- Table 1: State Summary
CREATE TABLE IF NOT EXISTS gold.order_summary (
    order_id UUID PRIMARY KEY,
    current_status TEXT,
    items JSONB,
    total_amount NUMERIC(10,2),
    last_updated_at TIMESTAMPTZ
);

-- Gold Table 2: Performance Tracking
CREATE TABLE IF NOT EXISTS gold.order_performance (
    order_id UUID PRIMARY KEY,
    order_placed_at TIMESTAMPTZ,
    order_completed_at TIMESTAMPTZ,
    cycle_time_seconds BIGINT,
    total_events INT
);

-- Table 3: Items Fact
CREATE TABLE IF NOT EXISTS gold.order_items_fact (
    id SERIAL PRIMARY KEY,
    order_id UUID,
    product_name TEXT,
    sale_timestamp TIMESTAMPTZ
);
