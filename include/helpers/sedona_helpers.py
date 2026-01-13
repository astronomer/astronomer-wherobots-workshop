from typing import Dict, List
from sedona.spark import SedonaContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, col, to_timestamp
import logging

logger = logging.getLogger(__name__)


def create_sedona_context(app_name: str = "SedonaApp") -> SedonaContext:
    sedona_jars = (
        "org.apache.sedona:sedona-spark-4.0_2.13:1.8.0,"
        "org.datasyslab:geotools-wrapper:1.8.0-33.1,"
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.777"
    )

    config = (
        SedonaContext.builder()
        .appName(app_name)
        .config("spark.jars.packages", sedona_jars)
        .config(
            "spark.jars.repositories",
            "https://artifacts.unidata.ucar.edu/repository/unidata-all",
        )
        .config(
            "spark.hadoop.fs.s3a.bucket.noaa-swdi-pds.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    
    return SedonaContext.create(config)


def load_noaa_swdi_data(sedona: SedonaContext, s3_uri: str) -> DataFrame:
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

    df = (
        sedona.read.option("comment", "#")
        .csv(s3_uri)
        .toDF(*column_names)
        .withColumn("timestamp", to_timestamp(col("ZTIME"), "yyyyMMddHHmmss"))
        .withColumn(
            "geometry",
            expr("ST_AsText(ST_Point(CAST(LON AS DOUBLE), CAST(LAT AS DOUBLE)))")
        )
    )
    
    return df


def filter_by_date_range(df: DataFrame, date: str) -> DataFrame:
    start_ztime = date + "000000"
    end_ztime = date + "235959"
    
    return df.where((col("ZTIME") >= start_ztime) & (col("ZTIME") <= end_ztime))


def fetch_noaa_data_from_s3(
    date: str,
    s3_uri: str,
    chunk_size: int = 10000
) -> List[List]:
    logger.info(f"Fetching data for: ({date})")

    sedona = create_sedona_context("NOAA-S3-Loader")
    
    df = load_noaa_swdi_data(sedona, s3_uri)
    df_filtered = filter_by_date_range(df, date)
    
    total_in_range = df_filtered.count()
    logger.info(f"Found {total_in_range:,} rows in date range ({date})")
    
    if total_in_range == 0:
        sedona.stop()
        logger.info(f"No data for ({date}), skipping")
        return []

    all_chunks = []
    for offset in range(0, total_in_range, chunk_size):
        rows_chunk = df_filtered.orderBy("ZTIME").limit(chunk_size).offset(offset).collect()
        logger.debug(f"Fetched chunk at offset {offset}: {len(rows_chunk)} rows")
        all_chunks.append(rows_chunk)
    
    sedona.stop()
    
    logger.info(f"Fetched total {total_in_range} rows for ({date})")
    return all_chunks


def create_merge_sql(batch: List, org_catalog: str, database: str) -> str:
    values_rows = []
    for row in batch:
        value_tuple = (
            f"('{row.ZTIME}', {row.LON}, {row.LAT}, '{row.WSR_ID}', '{row.CELL_ID}', "
            f"{row.RANGE}, {row.AZIMUTH}, {row.SEVPROB}, {row.PROB}, {row.MAXSIZE}, "
            f"CAST('{row.timestamp}' AS TIMESTAMP), ST_GeomFromText('{row.geometry}'))"
        )
        values_rows.append(value_tuple)

    return f"""
    MERGE INTO {org_catalog}.{database}.hail_events AS target
    USING (
        SELECT * FROM VALUES
        {', '.join(values_rows)}
        AS t(ZTIME, LON, LAT, WSR_ID, CELL_ID, RANGE, AZIMUTH, SEVPROB, PROB, MAXSIZE, timestamp, geometry)
    ) AS source
    ON target.ZTIME = source.ZTIME 
    AND target.WSR_ID = source.WSR_ID 
    AND target.CELL_ID = source.CELL_ID
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
    """
