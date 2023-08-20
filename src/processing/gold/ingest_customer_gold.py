from datetime import timedelta
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def ingest_customer_gold(
    df: DataFrame,
    customer_days_ago_to_calculate_orders: list[int],
    customer_columns_to_take: list[str],
) -> DataFrame:
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

    return df_customer_gold


def extract_overall_customer_information(
    df: DataFrame, customer_columns_to_calculate: list[str]
) -> DataFrame:
    aggregative_dict = {
        x: "first" for x in customer_columns_to_calculate if x != "CustomerID"
    }
    df = df.groupBy("CustomerID").agg(aggregative_dict)
    for column in customer_columns_to_calculate:
        df = df.withColumnRenamed(f"first({column})", column)
    return df


def calculate_overall_orders(df: DataFrame) -> DataFrame:
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
        .fillna(
            0, subset=[f"QuantityOfOrdersLast{number_of_days_ago_to_calculate}Days"]
        )
    )
    return df
