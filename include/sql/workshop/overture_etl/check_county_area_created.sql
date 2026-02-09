SELECT 
    postcode,
    country,
    ST_Area(ST_Transform(area, 'EPSG:4326', 'EPSG:3857')) / 2589988 as area_sqmi,
    created_at
FROM %(catalog)s.%(database)s.postcode_areas
WHERE postcode = '%(us_postcode)s' AND country = '%(country)s'