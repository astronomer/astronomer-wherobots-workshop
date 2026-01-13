SELECT 
    COUNT(*)                 AS total_rows,
    COUNT(DISTINCT WSR_ID)   AS unique_radars,
    MIN(timestamp)           AS earliest_event,
    MAX(timestamp)           AS latest_event,
    AVG(MAXSIZE)             AS avg_hail_size,
    MAX(MAXSIZE)             AS max_hail_size
FROM %(catalog)s.%(database)s.hail_events;