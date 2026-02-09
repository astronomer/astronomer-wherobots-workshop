CREATE TABLE IF NOT EXISTS %(catalog)s.%(database)s.postcode_areas (
    postcode STRING,
    country STRING,
    state_code STRING,
    area GEOMETRY,
    created_at TIMESTAMP
);