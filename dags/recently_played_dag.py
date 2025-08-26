from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.append('/opt/airflow')

# Import modular functions
from etl.recently_played.extract import get_access_token, get_last_window_timestamp_ms, fetch_recently_played, upload_raw, write_success_marker
from etl.recently_played.transform import download_raw, transform, upload_transformed
from etl.recently_played.load import download_processed, load_to_postgres


default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

@dag(
    dag_id="recently_played_dag",
    default_args=default_args,
    schedule="0 0,12 * * *",   # twice a day: midnight & noon
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spotify", "etl"],
)
def recently_played_dag():

    @task()
    def extract_task() -> str:
        logger.info("Starting extract_task...")

        token = get_access_token()
        logger.info("Got Spotify access token")

        after_ms, window_start = get_last_window_timestamp_ms(hours=12)
        prefix = window_start.strftime("%Y-%m-%d-%H")
        logger.info(f"Window start={window_start}, prefix={prefix}")

        records = fetch_recently_played(after_ms, token)
        logger.info(f"Fetched {len(records)} records")

        upload_raw(records, prefix)
        logger.info(f"Uploaded raw data with prefix={prefix}")

        write_success_marker("spotify-data", prefix)
        logger.info("Success marker written")

        return prefix

    @task()
    def transform_task(prefix: str) -> dict:
        logger.info(f"Starting transform_task for prefix={prefix}")
        df_raw = download_raw(prefix)
        logger.info(f"Downloaded raw data: {len(df_raw)} rows")

        df_clean = transform(df_raw)
        logger.info(f"Transformed data: {len(df_clean)} rows")

        return {"prefix": prefix, "df_clean": df_clean.to_json()}

    @task()
    def upload_transformed_task(data: dict) -> str:
        import pandas as pd
        prefix = data["prefix"]
        df_clean = pd.read_json(data["df_clean"])
        upload_transformed(df_clean, prefix)
        logger.info(f"Uploaded transformed data for prefix={prefix}")
        return prefix

    @task()
    def load_task(prefix: str):
        logger.info(f"Starting load_task for prefix={prefix}")
        df = download_processed(prefix)
        logger.info(f"Downloaded processed data: {len(df)} rows")

        load_to_postgres(df)
        logger.info(f"Loaded {len(df)} rows into Postgres successfully")

    # DAG flow
    prefix = extract_task()
    transform_result = transform_task(prefix)
    uploaded_prefix = upload_transformed_task(transform_result)
    load_task(uploaded_prefix)


recently_played_dag = recently_played_dag()