from typing import List, Tuple
from sedona.spark import SedonaContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StringType
import logging

logger = logging.getLogger(__name__)


def prepare_dataframe_for_kepler(
    sedona: SedonaContext,
    data: List[Tuple],
    columns: List[str]
) -> DataFrame:
    df = sedona.createDataFrame(data, columns)
    
    for col_name in ["LON", "LAT", "MAXSIZE"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
    
    if "timestamp" in df.columns:
        df = df.withColumn("timestamp", col("timestamp").cast(StringType()))
    
    return df


def create_kepler_map(
    sedona: SedonaContext,
    hail_data: List[Tuple],
    hail_columns: List[str],
    counties_data: List[Tuple],
    counties_columns: List[str],
    output_path: str
) -> int:
    from sedona.spark import SedonaKepler
    
    logger.info(f"Creating Kepler.gl map with {len(hail_data)} hail events")
    
    hail_df = prepare_dataframe_for_kepler(sedona, hail_data, hail_columns)
    counties_df = sedona.createDataFrame(counties_data, counties_columns)
    
    keplergl_map = SedonaKepler.create_map(hail_df, name="hail")
    SedonaKepler.add_df(keplergl_map, counties_df, name="counties")
    
    keplergl_map.save_to_html(file_name=output_path)
    
    logger.info(f"Map saved to {output_path}")
    
    return len(hail_data)
