from pyspark.sql import DataFrame

from src.utils.utils_functions import cast_str_column_to_date


def ingest_orders_gold(
    df: DataFrame, columns_to_be_renamed: dict[str, str], columns_to_select: list[str]
) -> DataFrame:
    df_with_renamed_columns = _rename_columns(
        df=df, columns_to_be_renamed=columns_to_be_renamed
    )
    df_with_casted_shipment_date = cast_str_column_to_date(
        df=df_with_renamed_columns, column_name="ShipmentDate"
    )
    df_with_chosen_columns = df_with_casted_shipment_date.select(*columns_to_select)
    return df_with_chosen_columns


def _rename_columns(df: DataFrame, columns_to_be_renamed: dict[str, str]) -> DataFrame:
    for old_column, new_column in columns_to_be_renamed.items():
        df = df.withColumnRenamed(old_column, new_column)
    return df
