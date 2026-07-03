from datetime import datetime
import subprocess

from airflow.sdk import dag, task


def run_command(command, cwd=None):
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"Commande echouee : {' '.join(command)}")


SNOWFLAKE_CONNECT_CODE = """
import os
import snowflake.connector as sc
conn = sc.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    role=os.environ["SNOWFLAKE_ROLE"],
)
conn.cursor().execute("{copy_statement}")
conn.close()
"""


@dag(
    dag_id="velib_load_dbt",
    description="Charge S3 vers RAW puis reconstruit staging et marts",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["velib", "dbt"],
)
def velib_load_dbt():

    @task(retries=2)
    def copy_into_status():
        copy_statement = (
            "COPY INTO VELIB_DB.RAW.STATION_STATUS_SNAPSHOTS "
            "FROM @VELIB_DB.RAW.S3_RAW_STAGE/velib_status/ "
            "FILE_FORMAT=(TYPE=PARQUET) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"
        )
        code = SNOWFLAKE_CONNECT_CODE.format(copy_statement=copy_statement)
        run_command(["python", "-c", code])

    @task(retries=2)
    def copy_into_stations():
        copy_statement = (
            "COPY INTO VELIB_DB.RAW.STATIONS "
            "FROM @VELIB_DB.RAW.S3_RAW_STAGE/velib_stations/ "
            "FILE_FORMAT=(TYPE=PARQUET) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"
        )
        code = SNOWFLAKE_CONNECT_CODE.format(copy_statement=copy_statement)
        run_command(["python", "-c", code])

    @task(retries=1)
    def run_dbt():
        run_command(["dbt", "run"], cwd="/opt/airflow/dbt_velib")

    [copy_into_status(), copy_into_stations()] >> run_dbt()


velib_load_dbt()