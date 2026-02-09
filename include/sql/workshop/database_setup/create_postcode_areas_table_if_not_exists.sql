CREATE TABLE IF NOT EXISTS %(catalog)s.%(database)s.postcode_areas (
    postcode STRING,
    country STRING,
    area GEOMETRY,
    created_at TIMESTAMP
);