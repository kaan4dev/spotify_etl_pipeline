import os
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import regexp_replace, from_json, explode, to_date, when, col, expr, year

RAW_DATA_FILE = os.path.join("data", "raw", "spotify.csv")
PROCESSED_DATA_DIR = os.path.join("data", "processed")

def transform_spotify_data():
    spark = SparkSession.builder.appName("SpotifyETL").getOrCreate()

    print(f"Reading Data From: {RAW_DATA_FILE}")
    df = spark.read.csv(RAW_DATA_FILE, header=True, inferSchema=True)

    df = df.select(
        "id",
        "name",
        "album",
        "album_id",
        "artists",
        "artist_ids",
        "track_number",
        "explicit",
        "danceability",
        "energy",
        "tempo",
        "year",
        "release_date"
    )

    df = df.withColumn("artist_ids_clean", from_json(regexp_replace("artist_ids", "'", '"'), ArrayType(StringType())))
    df = df.withColumn("artist_id", explode("artist_ids_clean"))

    df = df.withColumn(
        "release_date",
        when(col("release_date").rlike("^[0-9]{4}$"), expr("to_date(concat(release_date,'-01-01'), 'yyyy-MM-dd')"))
        .when(col("release_date").rlike("^[0-9]{4}-[0-9]{2}$"), expr("to_date(concat(release_date,'-01'), 'yyyy-MM-dd')"))
        .otherwise(to_date("release_date", "yyyy-MM-dd"))
    )

    # Drop placeholder dates (e.g. year 0000) that BigQuery rejects
    df = df.withColumn(
        "release_date",
        when(year("release_date") < 1900, None).otherwise(col("release_date"))
    )

    df = df.withColumnRenamed("id", "song_id") \
           .withColumnRenamed("name", "song_name") \
           .withColumnRenamed("artists", "artist")

    df = df.fillna({'song_name': 'Unknown'})

    df = df.withColumn("album", when(col("album").isNull(), col("song_name")).otherwise(col("album")))

    df = df.dropDuplicates(["song_id"])

    output_path = os.path.join(PROCESSED_DATA_DIR, "spotify_transformed")
    df.write.mode("overwrite").parquet(output_path)
    print(f"Transformed data written to: {output_path}")

    return df

if __name__ == "__main__":
    df = transform_spotify_data()
    df.show(10, truncate=False)
