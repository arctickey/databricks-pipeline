import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, TimestampType


def ingest_data(df: DataFrame, max_date_to_reload: str) -> DataFrame:
    df_with_changed_order_date_column = _alter_order_date_column(df=df)
    df_to_reload = df_with_changed_order_date_column.filter(
        F.col("Order Date") > F.lit(max_date_to_reload)
    )
    df_with_renamed_columns = _rename_columns(df=df_to_reload)

    return df_with_renamed_columns


def _rename_columns(df: DataFrame) -> DataFrame:
    for column in df.columns:
        new_column = column.replace("-", "").replace(" ", "")
        df = df.withColumnRenamed(column, new_column)
    return df


def _alter_order_date_column(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "Order Date",
        F.unix_timestamp(F.col("Order Date"), "dd/MM/yyyy")
        .cast(TimestampType())
        .cast(DateType()),
    )
