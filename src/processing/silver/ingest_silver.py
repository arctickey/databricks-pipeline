import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def ingest_silver(df: DataFrame) -> DataFrame:
    """
    Process the data into the silver level.
    Name of the customer is split into two separate columns.

    Args:
        df (DataFrame): Bronze level dataset.

    Returns:
        DataFrame: Silver level dataset.
    """
    df_with_extracted_customer_name = _extract_first_and_second_customer_name(df=df)
    return df_with_extracted_customer_name


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
