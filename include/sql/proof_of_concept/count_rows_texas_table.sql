SELECT 
    COUNT(*)                 AS total_events,
    MIN(timestamp)           AS earliest_event,
    MAX(timestamp)           AS latest_event,
    AVG(MAXSIZE)             AS avg_hail_size,
    MAX(MAXSIZE)             AS max_hail_size,
    COUNT(DISTINCT WSR_ID)   AS unique_radars
FROM %(catalog)s.%(database)s.%(table_name)s;