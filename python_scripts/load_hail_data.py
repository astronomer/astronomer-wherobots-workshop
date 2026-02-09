import argparse
from sedona.spark import *
from pyspark.sql.functions import expr, col, to_timestamp
from datetime import datetime
from dateutil.relativedelta import relativedelta


parser = argparse.ArgumentParser()
parser.add_argument("--catalog", type=str, default="org_catalog")
parser.add_argument("--database", type=str, default="workshop")
parser.add_argument("--end-date", type=str, required=True)
parser.add_argument("--lookback-months", type=int, default=3)
args = parser.parse_args()

CATALOG = args.catalog
DATABASE = args.database
END_DATE = args.end_date
LOOKBACK_MONTHS = args.lookback_months


config = (
    SedonaContext.builder()
    .config(
        "fs.s3a.bucket.noaa-swdi-pds.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    )
    .config(
        "spark.hadoop.fs.s3a.bucket.noaa-swdi-pds.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    )
    .getOrCreate()
)
sedona = SedonaContext.create(config)


end_date = datetime.strptime(END_DATE, "%Y-%m-%d").replace(day=1) - relativedelta(
    months=1
)  # first day of the last completed month

start_date = end_date - relativedelta(months=LOOKBACK_MONTHS)

current_year = datetime.now().year

# Collect S3 files to fetch: yearly for past years, monthly for current year
files_to_fetch = []
current = start_date
while current <= end_date:
    year = current.year
    month = current.month

    if year < current_year:
        # Past year - use yearly file (only add once per year)
        yearly_file = f"s3://noaa-swdi-pds/hail-{year}.csv"
        if yearly_file not in files_to_fetch:
            files_to_fetch.append(yearly_file)
    else:
        # Current year - use monthly file (YYYYMM format)
        monthly_file = f"s3://noaa-swdi-pds/hail-{year}{month:02d}.csv"
        files_to_fetch.append(monthly_file)

    current += relativedelta(months=1)

print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
print(f"Files to fetch: {files_to_fetch}")

column_names = [
    "ZTIME",
    "LON",
    "LAT",
    "WSR_ID",
    "CELL_ID",
    "RANGE",
    "AZIMUTH",
    "SEVPROB",
    "PROB",
    "MAXSIZE",
]

dfs = []
for s3_uri in files_to_fetch:
    print(f"Loading: {s3_uri}")
    df = (
        sedona.read.option("comment", "#")
        .csv(s3_uri)
        .toDF(*column_names)
        .withColumn("ZTIME", to_timestamp(col("ZTIME"), "yyyyMMddHHmmss"))
        .withColumn("geometry", expr("ST_Point(LON, LAT)"))
    )
    dfs.append(df)

hail_df = dfs[0]
for df in dfs[1:]:
    hail_df = hail_df.union(df)

hail_df = hail_df.filter((col("ZTIME") >= start_date) & (col("ZTIME") <= end_date))

hail_df.createOrReplaceTempView("hail_staging")

# upsert assuming sevprob, prob and maxsize can change for entries in the past 3 months
sedona.sql(
    f"""
    MERGE INTO {CATALOG}.{DATABASE}.hail_raw AS target
    USING (
        SELECT * FROM (
            SELECT 
                CAST(ZTIME AS TIMESTAMP) AS ZTIME,
                CAST(LON AS DOUBLE) AS LON,
                CAST(LAT AS DOUBLE) AS LAT,
                WSR_ID,
                CELL_ID,
                CAST(RANGE AS INT) AS RANGE,
                CAST(AZIMUTH AS INT) AS AZIMUTH,
                CAST(SEVPROB AS INT) AS SEVPROB,
                CAST(PROB AS INT) AS PROB,
                CAST(MAXSIZE AS DOUBLE) AS MAXSIZE,
                geometry,
                ROW_NUMBER() OVER (
                    PARTITION BY ZTIME, LON, LAT, WSR_ID, CELL_ID 
                    ORDER BY ZTIME
                ) AS rn
            FROM hail_staging
        ) WHERE rn = 1
    ) AS source
    ON target.ZTIME = source.ZTIME 
       AND target.LON = source.LON 
       AND target.LAT = source.LAT
       AND target.WSR_ID = source.WSR_ID
       AND target.CELL_ID = source.CELL_ID
    WHEN MATCHED THEN UPDATE SET
        SEVPROB = source.SEVPROB,
        PROB = source.PROB,
        MAXSIZE = source.MAXSIZE
    WHEN NOT MATCHED THEN INSERT (
        ZTIME, LON, LAT, WSR_ID, CELL_ID, RANGE, AZIMUTH, 
        SEVPROB, PROB, MAXSIZE, geometry
    ) VALUES (
        source.ZTIME, source.LON, source.LAT, source.WSR_ID, source.CELL_ID, 
        source.RANGE, source.AZIMUTH, source.SEVPROB, source.PROB, 
        source.MAXSIZE, source.geometry
    )
"""
)
