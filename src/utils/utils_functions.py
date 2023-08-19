from typing import List
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import datetime


def write_parquet(df: DataFrame, output_path: str, mode: str, partitionBy: list[str]):
    df_with_metadata_columns = add_metadata_columns(df=df)
    df_with_metadata_columns.write.parquet(
        output_path, mode=mode, partitionBy=partitionBy
    )
    return


def add_metadata_columns(df: DataFrame):
    df = df.withColumn("Filename", F.input_file_name())
    df = df.withColumn("ExecutionDatetime", F.current_timestamp())
    return df


def check_latest_dataframe_date(
    spark: SparkSession, output_path: str, date_column: str = "OrderDate"
):
    if_dataframe_exists = check_if_dataframe_exisits(spark.sparkContext, output_path)
    if if_dataframe_exists:
        max_date_existing = (
            spark.read.parquet(output_path)
            .agg({f"{date_column}": "max"})
            .collect()[0][0]
        )
    else:
        max_date_existing = datetime.date(2000, 1, 1).strftime("%Y-%m-%d")
    return max_date_existing


def check_if_dataframe_exisits(sc: SparkContext, path: str) -> bool:
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    if_path_exists = fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))
    return if_path_exists