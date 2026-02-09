MERGE INTO %(catalog)s.%(database)s.risk_comparison AS target
USING (
    WITH areas AS (
        SELECT 
            p.postcode,
            s.state_code,
            ST_Area(ST_Transform(p.area, 'EPSG:4326', 'EPSG:3857')) / 2589988 AS county_area_sqmi,
            ST_Area(ST_Transform(s.geometry, 'EPSG:4326', 'EPSG:3857')) / 2589988 AS state_area_sqmi
        FROM %(catalog)s.%(database)s.postcode_areas p, %(catalog)s.%(database)s.state_boundary s
        WHERE p.postcode = '%(us_postcode)s'
    ),
    
    county_stats AS (
        SELECT 
            'county' AS area_type,
            '%(us_postcode)s' AS area_id,
            COUNT(*) AS event_count,
            AVG(NULLIF(MAXSIZE, -999)) AS avg_hail_size,
            MAX(NULLIF(MAXSIZE, -999)) AS max_hail_size,
            AVG(NULLIF(SEVPROB, -999)) AS avg_severe_prob,
            AVG(NULLIF(PROB, -999)) AS avg_hail_prob,
            COUNT(CASE WHEN NULLIF(MAXSIZE, -999) >= 1.0 THEN 1 END) AS damaging_events,
            COUNT(CASE WHEN NULLIF(SEVPROB, -999) >= 50 THEN 1 END) AS high_risk_events
        FROM %(catalog)s.%(database)s.hail_county
        WHERE postcode = '%(us_postcode)s'
            AND ZTIME >= DATE_SUB(CURRENT_DATE(), 180)
    ),
    
    state_stats AS (
        SELECT 
            'state' AS area_type,
            a.state_code AS area_id,
            COUNT(*) AS event_count,
            AVG(NULLIF(MAXSIZE, -999)) AS avg_hail_size,
            MAX(NULLIF(MAXSIZE, -999)) AS max_hail_size,
            AVG(NULLIF(SEVPROB, -999)) AS avg_severe_prob,
            AVG(NULLIF(PROB, -999)) AS avg_hail_prob,
            COUNT(CASE WHEN NULLIF(MAXSIZE, -999) >= 1.0 THEN 1 END) AS damaging_events,
            COUNT(CASE WHEN NULLIF(SEVPROB, -999) >= 50 THEN 1 END) AS high_risk_events
        FROM %(catalog)s.%(database)s.hail_state h, areas a
        WHERE ZTIME >= DATE_SUB(CURRENT_DATE(), 180)
        GROUP BY a.state_code
    )
    
    SELECT 
        n.area_type,
        n.area_id,
        n.event_count,
        ROUND(n.event_count / NULLIF(a.county_area_sqmi, 0), 4) AS events_per_sqmi,
        ROUND(n.avg_hail_size, 4) AS avg_hail_size,
        n.max_hail_size,
        ROUND(n.avg_severe_prob, 4) AS avg_severe_prob,
        ROUND(n.avg_hail_prob, 4) AS avg_hail_prob,
        n.damaging_events,
        ROUND(n.damaging_events / NULLIF(a.county_area_sqmi, 0), 4) AS damaging_per_sqmi,
        n.high_risk_events,
        ROUND(n.high_risk_events / NULLIF(a.county_area_sqmi, 0), 4) AS high_risk_per_sqmi,
        ROUND(a.county_area_sqmi, 4) AS area_sqmi,
        CURRENT_TIMESTAMP() AS updated_at
    FROM county_stats n, areas a
    
    UNION ALL
    
    SELECT 
        s.area_type,
        s.area_id,
        s.event_count,
        ROUND(s.event_count / NULLIF(a.state_area_sqmi, 0), 4) AS events_per_sqmi,
        ROUND(s.avg_hail_size, 4) AS avg_hail_size,
        s.max_hail_size,
        ROUND(s.avg_severe_prob, 4) AS avg_severe_prob,
        ROUND(s.avg_hail_prob, 4) AS avg_hail_prob,
        s.damaging_events,
        ROUND(s.damaging_events / NULLIF(a.state_area_sqmi, 0), 4) AS damaging_per_sqmi,
        s.high_risk_events,
        ROUND(s.high_risk_events / NULLIF(a.state_area_sqmi, 0), 4) AS high_risk_per_sqmi,
        ROUND(a.state_area_sqmi, 4) AS area_sqmi,
        CURRENT_TIMESTAMP() AS updated_at
    FROM state_stats s, areas a
) AS source
ON target.area_type = source.area_type AND target.area_id = source.area_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *