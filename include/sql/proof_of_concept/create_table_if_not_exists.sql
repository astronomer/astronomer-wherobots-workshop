CREATE TABLE IF NOT EXISTS %(catalog)s.%(database)s.hail_events (
    ZTIME     STRING,
    LON       DOUBLE,
    LAT       DOUBLE,
    WSR_ID    STRING,
    CELL_ID   STRING,
    RANGE     INT,
    AZIMUTH   INT,
    SEVPROB   INT,
    PROB      INT,
    MAXSIZE   DOUBLE,
    timestamp TIMESTAMP,
    geometry  GEOMETRY
) USING ICEBERG;