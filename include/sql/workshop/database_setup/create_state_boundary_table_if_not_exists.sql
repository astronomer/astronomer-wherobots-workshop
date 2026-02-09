CREATE TABLE IF NOT EXISTS %(catalog)s.%(database)s.state_boundary (
    id STRING,
    state_name STRING,
    state_code STRING,
    geometry GEOMETRY
);