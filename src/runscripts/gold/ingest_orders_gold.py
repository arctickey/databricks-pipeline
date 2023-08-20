# %%
import pyspark.sql.functions as F

from src.config.config import Config
from src.config.config_gold import ConfigGold
from src.config.configure_environment import configure_environment
from src.processing.gold.ingest_orders_gold import ingest_orders_gold
from src.utils.logger import get_logger
from src.utils.utils_functions import check_latest_dataframe_date, write_parquet

logger = get_logger()

if __name__ == "__main__":
    try:
        spark = configure_environment()
        input_path = f"{Config.SILVER_DATA_PATH}/silver_data"
        output_path = f"{Config.GOLD_DATA_PATH}/gold_sales_data"
        max_date_to_reload = check_latest_dataframe_date(spark, output_path)
        df = spark.read.parquet(input_path).filter(
            F.col("OrderDate") > F.lit(max_date_to_reload)
        )
        if df.count() == 0:
            logger.info("No data to reload!")
        else:
            df_orders_gold = ingest_orders_gold(
                df=df,
                columns_to_be_renamed=ConfigGold.ORDERS_COLUMNS_TO_BE_RENAMED,
                columns_to_select=ConfigGold.ORDERS_COLUMNS_TO_SELECT,
            )

            logger.info(
                f"""
                Saving dataframe ({df_orders_gold.count()},
                {len(df_orders_gold.columns)}) shape to location = '{str(output_path)}'
                """
            )
            write_parquet(
                df=df_orders_gold,
                output_path=output_path,
                mode="append",
                partitionBy=["OrderDate"],
            )
    finally:
        logger.handlers[0].flush()

# %%
