from datetime import datetime
import subprocess

from airflow.sdk import dag, task


@dag(
    dag_id="velib_capture",
    description="Poll station_status toutes les 5 minutes et upload sur S3",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["velib", "capture"],
)
def velib_capture():

    @task(retries=2)
    def extract_station_status():
        result = subprocess.run(
            ["python", "/opt/airflow/extraction/extract_station_status.py"],
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError("Le script d'extraction a echoue")

    extract_station_status()


velib_capture()