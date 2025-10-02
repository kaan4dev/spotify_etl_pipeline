import os
from google.cloud import bigquery

MODELED_DATA_DIR = os.path.join("data", "modeled")
PROJECT_ID = "spotify-473920"
DATASET = "spotify_etl"

def load_to_bigquery():
    client = bigquery.Client(project=PROJECT_ID)

    tables = {
        "dim_artists": os.path.join(MODELED_DATA_DIR, "dim_artists"),
        "dim_albums": os.path.join(MODELED_DATA_DIR, "dim_albums"),
        "dim_dates": os.path.join(MODELED_DATA_DIR, "dim_dates"),
        "fact_songs": os.path.join(MODELED_DATA_DIR, "fact_songs"),
    }

    for table_name, path in tables.items():
        table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.PARQUET
        )

        print(f"Loading {table_name} into {table_id}...")

        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            if file_path.endswith(".parquet"):
                with open(file_path, "rb") as f:
                    load_job = client.load_table_from_file(f, table_id, job_config=job_config)
                    load_job.result()

        print(f"Loaded {table_name}")

if __name__ == "__main__":
    load_to_bigquery()
