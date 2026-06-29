#Extraction du référentiel des stations Velib (v1.0)
#Récupere station_information.json (quasi statique) 1x/jour
#et l'écrit en Parquet sur S3, partitionné par date uniquement
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from utils import upload_to_s3

load_dotenv()

station_information_url = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"


def extract_station_information():
    response = requests.get(station_information_url, timeout=30)
    response.raise_for_status()
    return response.json()


def to_dataframe(data):
    stations = data['data']['stations']
    df = pd.DataFrame(stations)

    colonnes_a_garder = [
        "station_id",
        "stationCode",
        "name",
        "lat",
        "lon",
        "capacity",
    ]
    return df[colonnes_a_garder]


def build_relative_path(now):
    date_str = now.strftime("%Y-%m-%d")
    return Path(f"date={date_str}") / "stations.parquet"


def main():
    now = datetime.now(timezone.utc)
    print(f"Extraction du référentiel des stations Velib à {now.isoformat()}")

    data = extract_station_information()
    df = to_dataframe(data)
    print(df.head())
    print(f"Shape: {df.shape}")

    relative_path = build_relative_path(now)
    local_path = Path("extraction/data/velib_stations") / relative_path
    local_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(local_path, index=False)
    print(f"Fichier Parquet sauvegardé localement dans {local_path}")

    bucket_name = os.environ["S3_BUCKET"]
    s3_key = f"raw/velib_stations/{relative_path.as_posix()}"
    upload_to_s3(local_path, s3_key, bucket_name)
    print(f"Fichier uploadé vers s3://{bucket_name}/{s3_key}")


if __name__ == "__main__":
    main()