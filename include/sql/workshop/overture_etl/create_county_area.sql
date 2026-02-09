MERGE INTO %(catalog)s.%(database)s.postcode_areas AS target
USING (
    WITH postcode_centroid AS (
        SELECT ST_Centroid(ST_Union_Aggr(geometry)) AS center
        FROM wherobots_open_data.overture_maps_foundation.addresses_address
        WHERE postcode = '%(us_postcode)s' AND country = '%(country)s'
    )
    SELECT 
        '%(us_postcode)s' AS postcode,
        '%(country)s' AS country,
        da.region AS state_code,
        da.geometry AS area,
        CURRENT_TIMESTAMP() AS created_at
    FROM wherobots_open_data.overture_maps_foundation.divisions_division_area da,
         postcode_centroid pc
    WHERE da.country = '%(country)s'
        AND da.subtype = 'county'
        AND ST_Contains(da.geometry, pc.center)
    LIMIT 1
) AS source
ON target.postcode = source.postcode AND target.country = source.country
WHEN MATCHED THEN UPDATE SET state_code = source.state_code, area = source.area, created_at = source.created_at
WHEN NOT MATCHED THEN INSERT *
;