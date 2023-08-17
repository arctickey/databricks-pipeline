from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv("/workspaces/ema/.env")


def configure_environment() -> SparkSession:
    """Starts the spark session"""
    spark = SparkSession.builder.appName("Hema task").getOrCreate()
    return spark
