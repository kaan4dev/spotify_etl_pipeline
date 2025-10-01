import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
    BooleanType, DateType
)

RAW_DATA_FILE = os.path.join("data", "raw", "spotify.csv")
PROCESSED_DATA_DIR = os.path.join("data", "processed")

def transform_spotify_data():
    spark = SparkSession.builder.appName("SpotifyETL").getOrCreate()

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("album", StringType(), True),
        StructField("album_id", StringType(), True),
        StructField("artists", StringType(), True),
        StructField("artist_ids", StringType(), True),
        StructField("track_number", IntegerType(), True),
        StructField("explicit", BooleanType(), True),
        StructField("danceability", DoubleType(), True),
        StructField("energy", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("year", IntegerType(), True),
        StructField("release_date", DateType(), True)
    ])

    print(f"Reading Data From: {RAW_DATA_FILE}")
    df = spark.read.csv(RAW_DATA_FILE, header= True, schema = schema)