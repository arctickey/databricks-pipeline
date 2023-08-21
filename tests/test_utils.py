# %%
import datetime

from pyspark.sql import SparkSession

from src.utils.utils_functions import (
    _add_metadata_columns,
    _check_if_dataframe_exisits,
    cast_str_column_to_date,
    check_latest_dataframe_date,
)

PATH = "/workspaces/ema/data/test"


def test_check_if_dataframe_not_exisits(spark: SparkSession) -> None:
    path = f"{PATH}/transaction_df"
    if_exists = _check_if_dataframe_exisits(spark.sparkContext, path)
    assert if_exists is False


def test_check_if_dataframe_exisits(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        data=[
            ("1", 1000.00),
            ("3", 3000.0),
        ],
        schema=["transaction_id", "amount"],
    )
    df.write.parquet(path=f"{PATH}/transaction_df")
    if_exists = _check_if_dataframe_exisits(
        spark.sparkContext, f"{PATH}/transaction_df"
    )
    assert if_exists is True


def test_add_metadata_columns(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        data=[
            ("1", 1000.00),
            ("3", 3000.0),
        ],
        schema=["transaction_id", "amount"],
    )
    df = _add_metadata_columns(df)
    assert "Filename" in df.columns and "ExecutionDatetime" in df.columns


def test_cast_str_column_to_date(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        data=[
            ("1", "01/12/2012"),
            ("3", "10/10/2020"),
        ],
        schema=["transaction_id", "date"],
    )
    df = cast_str_column_to_date(df=df, column_name="date")
    assert dict(df.dtypes)["date"] == "date"


def test_check_latest_dataframe_date(spark: SparkSession) -> None:
    path = f"{PATH}/date_df"
    df = spark.createDataFrame(
        [
            (1, datetime.date(year=2012, month=1, day=13)),
            (3, datetime.date(year=2020, month=10, day=10)),
        ],
        "transaction_id int, date date",
    )
    df.write.parquet(path)
    max_date = check_latest_dataframe_date(
        spark=spark, output_path=path, date_column="date"
    )
    assert max_date == "2020-10-10"


def test_check_latest_dataframe_date_not_exists(spark: SparkSession) -> None:
    path = f"{PATH}/date_not_exist_df"
    max_date = check_latest_dataframe_date(
        spark=spark, output_path=path, date_column="date"
    )
    assert max_date == "2000-01-01"
