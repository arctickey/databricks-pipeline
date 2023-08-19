from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def enrich_data(df: DataFrame):
    df_with_extracted_customer_name = _extract_first_and_second_customer_name(df=df)
    return df_with_extracted_customer_name


def _extract_first_and_second_customer_name(df: DataFrame) -> DataFrame:
    customer_name_column = F.split(df["CustomerName"], " ")
    df = df.withColumn("FirstName", customer_name_column.getItem(0))
    df = df.withColumn("SecondName", customer_name_column.getItem(1))
    df = df.drop("CustomerName")
    return df
