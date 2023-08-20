from pyspark.sql import DataFrame

from src.utils.utils_functions import cast_str_column_to_date


def ingest_orders_gold(
    df: DataFrame, columns_to_be_renamed: dict[str, str], columns_to_select: list[str]
) -> DataFrame:
    """
    Create gold level dataset with orders. Columns are renamed, so that
    they contain meaningful names. Also the `ShipmentDate` column is casted to date
    type.

    Args:
        df (DataFrame): Silver level dataset.
        columns_to_be_renamed (dict[str, str]): Dictionary with old and name names
            of the columns which are to be renamed.
        columns_to_select (list[str]): Set of columns to be chosen to the final
            dataset.

    Returns:
        DataFrame: Gold level dataset with orders
    """
    df_with_renamed_columns = _rename_columns(
        df=df, columns_to_be_renamed=columns_to_be_renamed
    )
    df_with_casted_shipment_date = cast_str_column_to_date(
        df=df_with_renamed_columns, column_name="ShipmentDate"
    )
    df_with_chosen_columns = df_with_casted_shipment_date.select(*columns_to_select)
    return df_with_chosen_columns


def _rename_columns(df: DataFrame, columns_to_be_renamed: dict[str, str]) -> DataFrame:
    """
    Rename the given set of columns according to the `columns_to_be_renamed` dict.
    """
    for old_column, new_column in columns_to_be_renamed.items():
        df = df.withColumnRenamed(old_column, new_column)
    return df
