CREATE TABLE IF NOT EXISTS %(catalog)s.%(database)s.risk_comparison (
    area_type STRING,
    area_id STRING,
    event_count BIGINT,
    events_per_sqmi DOUBLE,
    avg_hail_size DOUBLE,
    max_hail_size DOUBLE,
    avg_severe_prob DOUBLE,
    avg_hail_prob DOUBLE,
    damaging_events BIGINT,
    damaging_per_sqmi DOUBLE,
    high_risk_events BIGINT,
    high_risk_per_sqmi DOUBLE,
    area_sqmi DOUBLE,
    updated_at TIMESTAMP
);