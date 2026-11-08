
import json
import logging
from pathlib import Path
from pyspark.sql import SparkSession


# Create a Spark session for data processing
def create_spark_session(app_name="Project2_Spark"):
    return SparkSession.builder.appName(app_name).getOrCreate()


# Set up logging messages
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger("Project2_ETL")


# Read Spark settings from the configuration file
def load_spark_config(config_path="/opt/airflow/config/spark_config.json"):
    with open(config_path, "r") as file:
        return json.load(file)
