SELECT 
    COUNT(*) AS total_records,
    MIN(ZTIME) AS earliest_date,
    MAX(ZTIME) AS latest_date,
    COUNT(DISTINCT DATE(ZTIME)) AS unique_days,
    COUNT(DISTINCT WSR_ID) AS unique_stations,
    COUNT(CASE WHEN SEVPROB = -999 THEN 1 END) AS missing_sevprob,
    COUNT(CASE WHEN PROB = -999 THEN 1 END) AS missing_prob,
    COUNT(CASE WHEN MAXSIZE = -999 THEN 1 END) AS missing_maxsize,
    ROUND(AVG(NULLIF(MAXSIZE, -999)), 3) AS avg_hail_size,
    MAX(NULLIF(MAXSIZE, -999)) AS max_hail_size
FROM %(catalog)s.%(database)s.hail_raw
;