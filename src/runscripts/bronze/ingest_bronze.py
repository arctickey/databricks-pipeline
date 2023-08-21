# %%
from src.config.config import Config
from src.config.configure_environment import configure_environment
from src.processing.bronze.ingest_bronze import ingest_bronze
from src.utils.logger import get_logger
from src.utils.utils_functions import (
    check_latest_dataframe_date,
    detect_schema_change,
    write_parquet,
)

logger = get_logger()

if __name__ == "__main__":
    try:
        spark = configure_environment()
        input_path = f"{Config.RAW_DATA_PATH}/raw_data.csv"
        output_path = f"{Config.BRONZE_DATA_PATH}/bronze_data"
        max_date_to_reload = check_latest_dataframe_date(spark, output_path)
        old_columns = detect_schema_change(spark, output_path)

        df = spark.read.csv(input_path, header="true", inferSchema="true")
        df_bronze = ingest_bronze(df=df, max_date_to_reload=max_date_to_reload)

        if df_bronze.count() == 0:
            logger.info("No data to reload!")
        logger.info(
            f"""
            Saving dataframe ({df_bronze.count()},
            {len(df_bronze.columns)}) shape to location = '{str(output_path)}'
            """
        )
        logger.info(
            f"""
            Current schema is {df_bronze.printSchema()}
            Newly added columns to raw data are:
            {set(df_bronze.columns).difference(old_columns)}
            """
        )
        write_parquet(
            df=df_bronze,
            output_path=output_path,
            mode="append",
            partitionBy=["OrderDate"],
        )
    finally:
        logger.handlers[0].flush()

# %%
