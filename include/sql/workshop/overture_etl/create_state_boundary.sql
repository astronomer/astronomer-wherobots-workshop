MERGE INTO %(catalog)s.%(database)s.state_boundary AS target
USING (
    SELECT 
        da.id,
        d.names.primary AS state_name,
        d.region AS state_code,
        da.geometry
    FROM wherobots_open_data.overture_maps_foundation.divisions_division d
    JOIN wherobots_open_data.overture_maps_foundation.divisions_division_area da
        ON d.id = da.division_id
    WHERE d.subtype = 'region' 
        AND d.country = '%(country)s'
        AND d.region = CONCAT('%(country)s-', (
            SELECT address_levels[0].value
            FROM wherobots_open_data.overture_maps_foundation.addresses_address
            WHERE postcode = '%(us_postcode)s' AND country = '%(country)s'
            LIMIT 1
        ))
    LIMIT 1
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET 
    state_name = source.state_name,
    state_code = source.state_code,
    geometry = source.geometry
WHEN NOT MATCHED THEN INSERT *
;