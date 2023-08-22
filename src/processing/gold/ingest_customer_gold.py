from datetime import timedelta
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def ingest_customer_gold(
    df: DataFrame,
    customer_days_ago_to_calculate_orders: list[int],
    customer_columns_to_take: list[str],
) -> DataFrame:
    """
    Create gold level dataset with the information about customers. Contain columns
    with the summed number of orders, number of orders 5,10 and 15 days ago (this is set via
    `customer_days_ago_to_calculate_orders`) and other basic information about the customer, such as
    country or city (set via `customer_columns_to_take` parameter).
    Args:
        df (DataFrame): Silver level dataset.
        customer_days_ago_to_calculate_orders (list[int]): Number of days ago for which
        the columns with summed orders have to be created.
        customer_columns_to_take (list[str]): Set of columns to be chosen to the final
            dataset.
    Returns:
        DataFrame: Gold level dataset with customer data.
    """
    df_overall_number_of_orders = calculate_overall_orders(df=df)
    df_orders_x_days_ago = calculate_number_of_orders_for_past_dates_per_customer(
        df=df,
        customer_days_ago_to_calculate_orders=customer_days_ago_to_calculate_orders,
    )
    df_customer_overall_info = extract_overall_customer_information(
        df=df,
        customer_columns_to_calculate=[
            x for x in customer_columns_to_take if "Orders" not in x
        ],
    )
    df_customer_gold = df_customer_overall_info.join(
        df_orders_x_days_ago, on=["CustomerID"], how="left"
    ).join(df_overall_number_of_orders, on=["CustomerID"], how="left")

    columns_to_fill_with_zeros = [
        column for column in df_customer_gold.columns if "Quantity" in column
    ]
    df_customer_gold = df_customer_gold.fillna(0, columns_to_fill_with_zeros)

    return df_customer_gold


def extract_overall_customer_information(
    df: DataFrame, customer_columns_to_calculate: list[str]
) -> DataFrame:
    """
    Extract information such as country or city of each customer.
    """
    aggregative_dict = {
        x: "first" for x in customer_columns_to_calculate if x != "CustomerID"
    }
    df = df.groupBy("CustomerID").agg(aggregative_dict)
    for column in customer_columns_to_calculate:
        df = df.withColumnRenamed(f"first({column})", column)
    return df


def calculate_overall_orders(df: DataFrame) -> DataFrame:
    """
    Calculate total quantity of orders per each customer.
    """
    df_orders_summed = (
        df.groupBy("CustomerID")
        .count()
        .withColumnRenamed("count", "TotalQuantityOfOrders")
    )
    return (
        df_orders_summed.select(["CustomerID", "TotalQuantityOfOrders"])
        .drop_duplicates(
            subset=[
                "CustomerID",
            ]
        )
        .fillna(0, subset="TotalQuantityOfOrders")
    )


def calculate_number_of_orders_for_past_dates_per_customer(
    df: DataFrame, customer_days_ago_to_calculate_orders: list[int]
) -> DataFrame:
    """
    Calculate number of orders for each customer in the past days. Number of past days
    is set via `customer_days_ago_to_calculate_orders` parameter.
    """
    dataframes_with_orders_x_days_ago = []
    for number_of_days in customer_days_ago_to_calculate_orders:
        df_orders_x_days_ago = _calculate_number_of_orders_x_days_ago_per_customer(
            df=df, number_of_days_ago_to_calculate=number_of_days
        )
        dataframes_with_orders_x_days_ago.append(df_orders_x_days_ago)
    return reduce(
        lambda x, y: x.join(y, on="CustomerID", how="left"),
        dataframes_with_orders_x_days_ago,
    )


def _calculate_number_of_orders_x_days_ago_per_customer(
    df: DataFrame, number_of_days_ago_to_calculate: int
) -> DataFrame:
    """
    Helper function to calculate quantity of orders per each customer
    in the past `number_of_days_ago_to_calculate` days.
    """
    max_date = df.agg({"OrderDate": "max"}).collect()[0][0]
    date_to_calculate = max_date - timedelta(days=number_of_days_ago_to_calculate)
    df = (
        df.filter(F.col("OrderDate") > F.lit(date_to_calculate))
        .groupBy("CustomerID")
        .count()
        .withColumnRenamed(
            "count", f"QuantityOfOrdersLast{number_of_days_ago_to_calculate}Days"
        )
        .select(
            ["CustomerID", f"QuantityOfOrdersLast{number_of_days_ago_to_calculate}Days"]
        )
        .drop_duplicates(subset=["CustomerID"])
    )
    return df
