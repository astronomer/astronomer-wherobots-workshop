-- ToDo: modularize this sql to allow for different states

CREATE OR REPLACE TABLE %(catalog)s.%(database)s.%(table_name)s AS
SELECT h.*
FROM %(catalog)s.%(database)s.hail_events h
CROSS JOIN (
    SELECT 
        geometry              AS state_geometry, 
        ST_Envelope(geometry) AS bbox
    FROM wherobots_open_data.overture_maps_foundation.divisions_division_area
    WHERE subtype = 'region' 
      AND region = 'US-TX'
) texas
WHERE TO_DATE(h.timestamp) = DATE '%(subset_date)s'
  AND h.LAT >= 25.8  AND h.LAT <= 36.5 
  AND h.LON >= -106.6 AND h.LON <= -93.5
  AND ST_Intersects(h.geometry, texas.state_geometry);