import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, TimestampType


def write_parquet(
    df: DataFrame, output_path: str, mode: str, partitionBy: Optional[list[str]]
) -> None:
    """
    Create metadata columns and save the dataframe.
    Args:
        df (DataFrame): Dataframe to be saved
        output_path (str): Destination to which dataframe has to be saved
        mode (str): Whether the dataframe has to be appended or overwritten
        partitionBy (Optional[list[str]]): Via which columns dataframe has to be partitioned
    """
    df_with_metadata_columns = _add_metadata_columns(df=df)
    df_with_metadata_columns.write.parquet(
        output_path, mode=mode, partitionBy=partitionBy
    )
    return


def check_latest_dataframe_date(
    spark: SparkSession, output_path: str, date_column: str = "OrderDate"
) -> str:
    """
    Check whether dataframe exists and if so what is the latest `orderDate`
    (the column can be changed via `date_column` parameter).
    If dataframe does not exists then 01.01.2000 is set as the date from
    which table has to be reloaded.

    Args:
        spark (SparkSession): spark session
        output_path (str): path under which existence of the table has to be checked
        date_column (str, optional): Column with dates which has to be
            checked for reloading. Defaults to "OrderDate".

    Returns:
        str: date in "%Y-%m-%d" format with the data from when to reload the table
    """
    if_dataframe_exists = _check_if_dataframe_exisits(spark.sparkContext, output_path)
    if if_dataframe_exists:
        max_date_existing = (
            spark.read.parquet(output_path)
            .agg({f"{date_column}": "max"})
            .collect()[0][0]
        )
    else:
        max_date_existing = datetime.date(2000, 1, 1)
    return str(max_date_existing.strftime("%Y-%m-%d"))


def cast_str_column_to_date(
    df: DataFrame, column_name: str, date_format: str = "dd/MM/yyyy"
) -> DataFrame:
    """
    Cast string `column_name` which contains date in `date_format` to the date format.
    """
    return df.withColumn(
        column_name,
        F.unix_timestamp(F.col(column_name), date_format)
        .cast(TimestampType())
        .cast(DateType()),
    )


def _check_if_dataframe_exisits(sc: SparkContext, path: str) -> bool:
    """
    Check whether dataframe under `path` exists.
    """
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    if_path_exists = bool(fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path)))
    return if_path_exists


def _add_metadata_columns(df: DataFrame) -> DataFrame:
    """
    Add input file name and execution time to the saved dataframe.
    """
    df = df.withColumn("Filename", F.input_file_name())
    df = df.withColumn("ExecutionDatetime", F.current_timestamp())
    return df
