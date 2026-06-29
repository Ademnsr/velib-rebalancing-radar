#Extraction du status des stations Velib (v4.0)
#Récupere le JSON de l'API Velib, le transforme en table, l'écrit en Parquet
#partitionné par date/heure, puis l'upload vers S3
import os
from datetime import datetime, timezone
from pathlib import Path
from utils import upload_to_s3
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

station_status_url = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"


def extract_station_status():
    response = requests.get(station_status_url, timeout=30)
    response.raise_for_status()
    return response.json()


def to_dataframe(data):
    stations = data['data']['stations']
    df = pd.DataFrame(stations)

    colonnes_a_garder = [
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "is_installed",
        "is_renting",
        "is_returning",
        "last_reported",
    ]
    return df[colonnes_a_garder]


def build_relative_path(now):
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")
    filename = f"status_{now.strftime('%H%M')}.parquet"
    return Path(f"date={date_str}") / f"hour={hour_str}" / filename

def main():
    now = datetime.now(timezone.utc)
    print(f"Extraction du status des stations Velib à {now.isoformat()}")

    data = extract_station_status()
    df = to_dataframe(data)
    print(df.head())
    print(f"Shape: {df.shape}")

    relative_path = build_relative_path(now)
    local_path = Path("extraction/data") / relative_path
    local_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(local_path, index=False)
    print(f"Fichier Parquet sauvegardé localement dans {local_path}")

    bucket_name = os.environ["S3_BUCKET"]
    s3_key = f"raw/velib_status/{relative_path.as_posix()}"
    upload_to_s3(local_path, s3_key, bucket_name)
    print(f"Fichier uploadé vers s3://{bucket_name}/{s3_key}")


if __name__ == "__main__":
    main()