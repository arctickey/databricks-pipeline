import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from src.utils.utils_functions import cast_str_column_to_date


def ingest_bronze(df: DataFrame, max_date_to_reload: str) -> DataFrame:
    """
    Process raw data by changing column names to parquet and casting order date
    column to date type. Table is reloaded in the incremental manner, meaning that
    only dates which are not present in the already saved dataset are processed.

    Args:
        df (DataFrame): dataframe with raw data
        max_date_to_reload (str): latest date present in the already saved bronze dataframe

    Returns:
        DataFrame: Bronze level dataframe
    """
    df_with_changed_order_date_column = cast_str_column_to_date(
        df=df, column_name="Order Date"
    )

    df_to_reload = df_with_changed_order_date_column.filter(
        F.col("Order Date") > F.lit(max_date_to_reload)
    )
    df_with_renamed_columns = _cast_to_snake_case(df=df_to_reload)

    return df_with_renamed_columns


def _cast_to_snake_case(df: DataFrame) -> DataFrame:
    """
    Cast column names to snake case.
    """
    for column in df.columns:
        new_column = column.replace("-", "").replace(" ", "")
        df = df.withColumnRenamed(column, new_column)
    return df
