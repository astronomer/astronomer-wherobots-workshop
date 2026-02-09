from sedona.spark import SedonaContext
from sedona.spark.maps.SedonaKepler import SedonaKepler
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", type=str, default="org_catalog")
parser.add_argument("--database", type=str, default="workshop")
parser.add_argument("--postcode", type=str, default="75001")
parser.add_argument("--s3-uri", type=str, required=True)
args = parser.parse_args()

CATALOG = args.catalog
DATABASE = args.database
US_POSTCODE = args.postcode
S3_URI = args.s3_uri

config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

worst_day = sedona.sql(
    f"""
    SELECT 
        DATE(ZTIME) as date,
        COUNT(*) as event_count,
        MAX(NULLIF(MAXSIZE, -999)) as max_size
    FROM {CATALOG}.{DATABASE}.hail_county
    WHERE postcode = '{US_POSTCODE}'
    GROUP BY DATE(ZTIME)
    ORDER BY max_size DESC, event_count DESC
    LIMIT 1
"""
).collect()[0]

worst_date = worst_day["date"]

state_hail_worst_day_df = sedona.sql(
    f"""
    SELECT 
        CAST(ZTIME AS STRING) as ZTIME,
        LON,
        LAT,
        MAXSIZE,
        SEVPROB,
        PROB,
        geometry,
        CAST(DATE(ZTIME) AS STRING) as date
    FROM {CATALOG}.{DATABASE}.hail_state
    WHERE DATE(ZTIME) = '{worst_date}'
"""
)

state_boundary = sedona.sql(
    f"SELECT state_code FROM {CATALOG}.{DATABASE}.state_boundary"
).collect()[0]["state_code"]

counties_df = (
    sedona.table("wherobots_open_data.overture_maps_foundation.divisions_division_area")
    .where(col("subtype") == "county")
    .where(col("region") == state_boundary)
    .select("geometry", col("names.primary").alias("county_name"))
)

county_name = sedona.sql(
    f"""
    SELECT da.names.primary as county_name
    FROM wherobots_open_data.overture_maps_foundation.divisions_division_area da,
         {CATALOG}.{DATABASE}.postcode_areas p
    WHERE da.subtype = 'county' 
      AND da.country = 'US'
      AND ST_Equals(da.geometry, p.area)
      AND p.postcode = '{US_POSTCODE}'
"""
).collect()

county_label = (
    county_name[0]["county_name"] if county_name else f"County ({US_POSTCODE})"
)

selected_county_df = sedona.sql(
    f"""
    SELECT area as geometry, postcode
    FROM {CATALOG}.{DATABASE}.postcode_areas
    WHERE postcode = '{US_POSTCODE}'
"""
)

map_center = sedona.sql(
    f"""
    SELECT 
        ST_X(ST_Centroid(geometry)) as lon,
        ST_Y(ST_Centroid(geometry)) as lat
    FROM {CATALOG}.{DATABASE}.state_boundary
"""
).collect()[0]

center_lon = float(map_center["lon"])
center_lat = float(map_center["lat"])

map_config = {
    "version": "v1",
    "config": {
        "mapState": {
            "latitude": center_lat,
            "longitude": center_lon,
            "zoom": 6,
        },
        "mapStyle": {"styleType": "dark"},
    },
}

worst_day_map = SedonaKepler.create_map(state_hail_worst_day_df, name="hail")
SedonaKepler.add_df(worst_day_map, counties_df, name="counties")
SedonaKepler.add_df(worst_day_map, selected_county_df, name="selected_county")

local_path = f"/tmp/visualization_{US_POSTCODE}.html"
worst_day_map.save_to_html(file_name=local_path)

output_path = f"{S3_URI}/visualization_{US_POSTCODE}.html"


spark = SparkSession.builder.getOrCreate()


hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jvm.java.net.URI(output_path), hadoop_conf
)
output_path_obj = spark._jvm.org.apache.hadoop.fs.Path(output_path)


if fs.exists(output_path_obj):
    fs.delete(output_path_obj, True)

output_stream = fs.create(output_path_obj, True)


with open(local_path, "rb") as f:
    html_bytes = f.read()

output_stream.write(bytearray(html_bytes))
output_stream.close()

print(f"Visualization saved to {output_path}")
