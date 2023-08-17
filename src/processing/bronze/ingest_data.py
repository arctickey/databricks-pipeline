import pyspark.sql.functions as F
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql import DataFrame


def ingest_data(df: DataFrame) -> DataFrame:
    df_with_metadata_columns = _add_metadata_columns(df=df)
    df_with_changed_order_date_column = _alter_order_date_column(
        df=df_with_metadata_columns
    )
    df_with_renamed_columns = _rename_columns(df=df_with_changed_order_date_column)
    return df_with_renamed_columns


def _add_metadata_columns(df: DataFrame):
    df = df.withColumn("Filename", F.input_file_name())
    df = df.withColumn("ExecutionDatetime", F.current_timestamp())
    return df


def _alter_order_date_column(df: DataFrame):
    return df.withColumn(
        "Order Date",
        F.unix_timestamp(F.col("Order Date"), "dd/MM/yyyy")
        .cast(TimestampType())
        .cast(DateType()),
    )


def _rename_columns(df: DataFrame):
    for column in df.columns:
        new_column = column.replace("-", "").replace(" ", "")
        df = df.withColumnRenamed(column, new_column)
    return df
