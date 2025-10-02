import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col

PROCESSED_DATA_DIR = os.path.join("data", "processed")
MODELED_DATA_DIR = os.path.join("data", "modeled")

def create_models():
    spark = SparkSession.builder.appName("SpotifyModeling").getOrCreate()

    input_path = os.path.join(PROCESSED_DATA_DIR, "spotify_transformed")
    df = spark.read.parquet(input_path)

    dim_artists = df.select("artist_id", "artist").dropDuplicates(["artist_id"])

    dim_albums = df.select("album_id", "album").dropDuplicates(["album_id"])

    dim_dates = (
        df.select("release_date")
          .withColumn("year", year("release_date"))
          .withColumn("month", month("release_date"))
          .withColumn("day", dayofmonth("release_date"))
          .dropDuplicates(["release_date"])
          .filter(col("release_date").isNotNull())
          .filter(col("year") >= 1900)  
    )

    fact_songs = (
        df.select(
            "song_id",
            "song_name",
            "artist_id",
            "album_id",
            "release_date",
            "danceability",
            "energy",
            "tempo",
            "explicit",
            "track_number",
            "year"
        )
        .filter(col("release_date").isNotNull())
        .filter(year("release_date") >= 1900)
    )

    dim_artists.write.mode("overwrite").parquet(os.path.join(MODELED_DATA_DIR, "dim_artists"))
    dim_albums.write.mode("overwrite").parquet(os.path.join(MODELED_DATA_DIR, "dim_albums"))
    dim_dates.write.mode("overwrite").parquet(os.path.join(MODELED_DATA_DIR, "dim_dates"))
    fact_songs.write.mode("overwrite").parquet(os.path.join(MODELED_DATA_DIR, "fact_songs"))

    print(f"Modeling completed. Data written to: {MODELED_DATA_DIR}")

if __name__ == "__main__":
    create_models()
