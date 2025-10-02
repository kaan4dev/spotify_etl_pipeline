import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

PROCESSED_DATA_DIR = os.path.join("data", "processed")

def create_star_schema(df):
    