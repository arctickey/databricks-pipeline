from pyspark.sql import DataFrame


def ingest_orders_gold(df: DataFrame, columns_to_select: list[str]) -> DataFrame:
    """
    Create gold level dataset with orders.

    Args:
        df (DataFrame): Silver level dataset.

    Returns:
        DataFrame: Gold level dataset with orders
    """
    df_with_chosen_columns = df.select(*columns_to_select).drop_duplicates(["OrderID"])
    return df_with_chosen_columns
