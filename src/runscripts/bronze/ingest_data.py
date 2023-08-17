# %%
from src.config.configure_environment import configure_environment
from src.config.config import Config
from src.processing.bronze.ingest_data import ingest_data
from pathlib import Path
from src.utils.logger import get_logger

# %%
logger = get_logger()

if __name__ == "__main__":
    spark = configure_environment()

    input_path = Path(f"{Config.RAW_DATA_PATH}/raw_data.csv")
    df = spark.read.csv(input_path, header="true", inferSchema="true")
    df_bronze = ingest_data(df=df)

    output_path = Path(f"{Config.BRONZE_DATA_PATH}/bronze_data")
    logger.info(
        f"""
        Saving dataframe ({df_bronze.count()},
        {len(df_bronze.columns)}) shape to location = '{str(output_path)}'
        """
    )
    df_bronze.write.parquet(
        output_path,
        partitionBy=["OrderDate"],
        mode="overwrite",
    )
