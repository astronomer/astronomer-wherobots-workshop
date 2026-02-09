CREATE TABLE IF NOT EXISTS %(catalog)s.%(database)s.hail_state (
    ZTIME TIMESTAMP,
    LON DOUBLE,
    LAT DOUBLE,
    WSR_ID STRING,
    CELL_ID STRING,
    RANGE INT,
    AZIMUTH INT,
    SEVPROB INT,
    PROB INT,
    MAXSIZE DOUBLE,
    geometry GEOMETRY,
    postcode STRING,
    state_code STRING
);