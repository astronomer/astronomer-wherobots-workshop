import argparse
from sedona.spark import SedonaContext

parser = argparse.ArgumentParser()
parser.add_argument('--catalog', type=str, required=True)
parser.add_argument('--database', type=str, required=True)
parser.add_argument('--postcode', type=str, required=True)
parser.add_argument('--country', type=str, required=True)
args = parser.parse_args()

CATALOG = args.catalog
DATABASE = args.database
POSTCODE = args.postcode
COUNTRY = args.country

config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

center = sedona.sql(f"""
    SELECT ST_AsText(ST_Centroid(ST_Union_Aggr(geometry))) AS center_wkt
    FROM wherobots_open_data.overture_maps_foundation.addresses_address
    WHERE postcode = '{POSTCODE}' AND country = '{COUNTRY}'
""").collect()[0]['center_wkt']

print(f"Postcode {POSTCODE} centroid: {center}")

sedona.sql(f"""
    MERGE INTO {CATALOG}.{DATABASE}.postcode_areas AS target
    USING (
        SELECT 
            '{POSTCODE}' AS postcode,
            '{COUNTRY}' AS country,
            geometry AS area,
            CURRENT_TIMESTAMP() AS created_at
        FROM wherobots_open_data.overture_maps_foundation.divisions_division_area
        WHERE country = '{COUNTRY}'
            AND subtype = 'county'
            AND ST_Contains(geometry, ST_GeomFromText('{center}'))
        LIMIT 1
    ) AS source
    ON target.postcode = source.postcode AND target.country = source.country
    WHEN MATCHED THEN UPDATE SET area = source.area, created_at = source.created_at
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"County area for postcode {POSTCODE} created/updated successfully")
