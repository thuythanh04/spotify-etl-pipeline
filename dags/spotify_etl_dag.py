from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
from airflow.decorators import task
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.append('/opt/airflow') 

# Import modular functions
from etl.extract import get_access_token, get_yesterday_timestamp_ms, fetch_recently_played, upload_raw, write_success_marker
from etl.transform import download_raw, transform, upload_transformed
from etl.load import download_processed, load_to_postgres


default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

@dag(
    dag_id="spotify_etl_modular",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spotify", "etl"],
)
def spotify_etl_modular():

    @task()
    def extract_task():
        logger.info("Starting extract_task...")
        try:
            logger.info("Getting Spotify access token...")
            token = get_access_token()
            logger.info("Got access token successfully")
        except Exception as e:
            logger.error("Failed at get_access_token()", exc_info=True)
            raise

        try:
            logger.info("Computing yesterday timestamp...")
            after_ms, yesterday = get_yesterday_timestamp_ms()
            logger.info(f"Timestamp computed: after_ms={after_ms}, yesterday={yesterday}")
        except Exception as e:
            logger.error("Failed at get_yesterday_timestamp_ms()", exc_info=True)
            raise

        try:
            logger.info("Fetching recently played tracks from Spotify...")
            records = fetch_recently_played(after_ms, token)
            logger.info(f"Fetched {len(records)} records")
        except Exception as e:
            logger.error("Failed at fetch_recently_played()", exc_info=True)
            raise

        try:
            logger.info("Uploading raw data...")
            date_prefix = upload_raw(records, yesterday)
            logger.info(f"Uploaded raw data with prefix: {date_prefix}")
        except Exception as e:
            logger.error("Failed at upload_raw()", exc_info=True)
            raise

        try:
            logger.info("Writing success marker...")
            write_success_marker("spotify-data", date_prefix)
            logger.info("Success marker written")
        except Exception as e:
            logger.error("Failed at write_success_marker()", exc_info=True)
            raise

        logger.info("extract_task completed successfully")
        return date_prefix


    @task()
    def transform_task(date_prefix: str):
        logger.info(f"Starting transform_task for prefix={date_prefix}...")
        try:
            df_raw = download_raw(date_prefix)
            logger.info(f"Downloaded raw data: {len(df_raw)} rows")
        except Exception:
            logger.error("Failed at download_raw()", exc_info=True)
            raise

        try:
            df_clean = transform(df_raw)
            logger.info(f"Transformed data: {len(df_clean)} rows")
        except Exception:
            logger.error("Failed at transform()", exc_info=True)
            raise

        logger.info("transform_task completed successfully")
        return {"date_prefix": date_prefix, "df_clean": df_clean.to_json()}  


    @task()
    def upload_transformed_task(data: dict):
        import pandas as pd
        logger.info(f"Starting upload_transformed_task for prefix={data['date_prefix']}...")
        try:
            df_clean = pd.read_json(data["df_clean"])
            upload_transformed(df_clean, data["date_prefix"])
            logger.info(f"Uploaded transformed data: {len(df_clean)} rows, prefix={data['date_prefix']}")
        except Exception:
            logger.error("Failed at upload_transformed()", exc_info=True)
            raise
        logger.info("upload_transformed_task completed successfully")
        return data["date_prefix"]


    @task()
    def load_task(date_prefix: str):
        logger.info(f"Starting load_task for prefix={date_prefix}...")
        try:
            df = download_processed(date_prefix)
            logger.info(f"Downloaded processed data: {len(df)} rows")
        except Exception:
            logger.error("Failed at download_processed()", exc_info=True)
            raise

        try:
            load_to_postgres(df)
            logger.info(f"Loaded {len(df)} rows into Postgres successfully")
        except Exception:
            logger.error("Failed at load_to_postgres()", exc_info=True)
            raise

        logger.info("load_task completed successfully")


    date_prefix = extract_task()
    transform_result = transform_task(date_prefix)
    uploaded_date_prefix = upload_transformed_task(transform_result)
    load_task(uploaded_date_prefix)


spotify_etl_modular_dag = spotify_etl_modular()