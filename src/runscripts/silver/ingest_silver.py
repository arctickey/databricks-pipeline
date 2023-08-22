# %%
import pyspark.sql.functions as F

from src.config.config import Config
from src.config.configure_environment import configure_environment
from src.processing.silver.ingest_silver import ingest_silver
from src.utils.logger import get_logger
from src.utils.utils_functions import check_latest_dataframe_date, write_parquet

logger = get_logger()

if __name__ == "__main__":
    try:
        spark = configure_environment()
        input_path = f"{Config.BRONZE_DATA_PATH}/bronze_data"
        output_path = f"{Config.SILVER_DATA_PATH}/silver_data"
        max_date_to_reload = check_latest_dataframe_date(spark, output_path)
        df = (
            spark.read.option("mergeSchema", "true")
            .parquet(input_path)
            .filter(F.col("OrderDate") > F.lit(max_date_to_reload))
        )
        if df.count() == 0:
            logger.info("No data to reload!")
        else:
            df_silver = ingest_silver(
                df=df, columns_to_be_renamed=Config.COLUMNS_TO_BE_RENAMED
            )
            logger.info(
                f"""
                Saving dataframe ({df_silver.count()},
                {len(df_silver.columns)}) shape to location = '{str(output_path)}'
                """
            )
            write_parquet(
                df=df_silver,
                output_path=output_path,
                mode="append",
                partitionBy=["OrderDate"],
            )
    finally:
        logger.handlers[0].flush()

# %%
