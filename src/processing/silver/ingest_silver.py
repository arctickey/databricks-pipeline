import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from src.utils.utils_functions import cast_str_column_to_date


def ingest_silver(df: DataFrame, columns_to_be_renamed: dict[str, str]) -> DataFrame:
    """
    Process the data into the silver level.
    Name of the customer is split into two separate columns.
    Columns are renamed, so that
    they contain meaningful names. Also the `ShipmentDate` column is casted to date
    type.

    Args:
        df (DataFrame): Bronze level dataset.
        columns_to_be_renamed (dict[str, str]): Dictionary with old and name names
            of the columns which are to be renamed.
        columns_to_select (list[str]): Set of columns to be chosen to the final
            dataset.


    Returns:
        DataFrame: Silver level dataset.
    """
    df_with_extracted_customer_name = _extract_first_and_second_customer_name(df=df)
    df_with_renamed_columns = _rename_columns(
        df=df_with_extracted_customer_name, columns_to_be_renamed=columns_to_be_renamed
    )
    df_with_casted_shipment_date = cast_str_column_to_date(
        df=df_with_renamed_columns, column_name="ShipmentDate"
    )
    return df_with_casted_shipment_date


def _rename_columns(df: DataFrame, columns_to_be_renamed: dict[str, str]) -> DataFrame:
    """
    Rename the given set of columns according to the `columns_to_be_renamed` dict.
    """
    for old_column, new_column in columns_to_be_renamed.items():
        df = df.withColumnRenamed(old_column, new_column)
    return df


def _extract_first_and_second_customer_name(df: DataFrame) -> DataFrame:
    """
    Split `CustomerName` column into two, which contain first and last name of
    the customer.
    """
    customer_name_column = F.split(df["CustomerName"], " ")
    df = df.withColumn("CustomerFirstName", customer_name_column.getItem(0))
    df = df.withColumn("CustomerLastName", customer_name_column.getItem(1))
    df = df.drop("CustomerName")
    return df
