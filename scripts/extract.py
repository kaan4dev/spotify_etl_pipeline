import os
import pandas as pd

RAW_DATA_DIR = os.path.join("data", "raw")
RAW_DATA_FILE = os.path.join(RAW_DATA_DIR, "spotify.csv")

def extract_spotify_data(input_path: str):
    os.makedirs(RAW_DATA_DIR, exist_ok= True)


    try:
        df = pd.read_csv(input_path)

    except FileNotFoundError:
        print(f"[ERROR] Input file not found: {input_path}")
        raise

    except Exception as e:
        print(f"[ERROR] Could not read {input_path}: {e}")
        raise

    print(f"[INFO] Extracted {df.shape[0]} rows and {df.shape[1]} columns.")

    df.to_csv(RAW_DATA_FILE, index=False)
    print(f"[INFO] Saved raw data to {RAW_DATA_FILE}")

    return RAW_DATA_FILE

if __name__ == "__main__":
    input_path = "/Users/kaancakir/Downloads/tracks_features.csv"
    extract_spotify_data(input_path)