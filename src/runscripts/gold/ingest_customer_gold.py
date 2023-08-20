# %%
from src.config.config import Config
from src.config.config_gold import ConfigGold
from src.config.configure_environment import configure_environment
from src.processing.gold.ingest_customer_gold import ingest_customer_gold
from src.utils.logger import get_logger
from src.utils.utils_functions import write_parquet

logger = get_logger()

if __name__ == "__main__":
    try:
        spark = configure_environment()
        input_path = f"{Config.SILVER_DATA_PATH}/silver_data"
        output_path = f"{Config.GOLD_DATA_PATH}/gold_customer_data"
        df = spark.read.parquet(input_path)
        df_customer_gold = ingest_customer_gold(
            df=df,
            customer_days_ago_to_calculate_orders=ConfigGold.CUSTOMER_DAYS_AGO_TO_CALCULATE_ORDERS,
            customer_columns_to_take=ConfigGold.CUSTOMER_COLUMNS_TO_SELECT,
        )
        logger.info(
            f"""
            Saving dataframe ({df_customer_gold.count()},
            {len(df_customer_gold.columns)}) shape to location = '{str(output_path)}'
            """
        )
        write_parquet(
            df=df_customer_gold,
            output_path=output_path,
            mode="overwrite",
            partitionBy=None,
        )
    finally:
        logger.handlers[0].flush()

# %%
