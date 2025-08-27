import json
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import datetime
import logging
from config import MINIO_BUCKET
from etl.utils.minio_utils import init_minio_client

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_last_window_timestamp_ms(hours: int = 12) -> tuple[int, datetime.datetime]:
    """
    Returns the timestamp (ms) for the start of the last ETL window,
    based on the given interval in hours.
    """
    now = datetime.datetime.now()
    window_start = now - datetime.timedelta(hours=hours)

    logging.info(f"Computed ETL window start: {window_start}")
    return int(window_start.timestamp() * 1000), window_start


def download_raw(date_prefix: str) -> pd.DataFrame:
    logger.info(f"Downloading raw data for date_prefix={date_prefix}")
    client = init_minio_client()
    raw_path = f"raw/{date_prefix}/recently_played.json"

    try:
        response = client.get_object(MINIO_BUCKET, raw_path)
        data = json.loads(response.read().decode("utf-8"))
        logger.info(f"Downloaded raw JSON with {len(data)} records from {raw_path}")
    except Exception as e:
        logger.error(f"Failed to download raw data from {raw_path}", exc_info=True)
        raise
    finally:
        response.close()
        response.release_conn()

    return pd.DataFrame(data)


def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Validating and cleaning dataframe")
    if df.empty:
        logger.error("Dataset is empty")
        raise ValueError("Dataset empty!")

    before = len(df)
    df = df.dropna(how="all")
    df = df.dropna(subset=["song_duration_ms", "played_at"], how="any")
    after = len(df)
    logger.info(f"Dropped {before - after} invalid rows; {after} rows remain")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming dataframe")
    df = validate_and_clean(df)

    df["song_duration_ms"] = df["song_duration_ms"].astype(int)

    # Ensure UTC timezone-awareness
    df["played_at"] = pd.to_datetime(df["played_at"], errors="coerce", utc=True)

    before = len(df)
    df = df.drop_duplicates(subset=["song_id", "played_at"])
    after = len(df)
    logger.info(f"Removed {before - after} duplicate rows; {after} rows remain")

    # Add local-time derived fields (VN)
    df["played_at_local"] = df["played_at"].dt.tz_convert("Asia/Ho_Chi_Minh")
    df["year"] = df["played_at_local"].dt.year
    df["month"] = df["played_at_local"].dt.month
    df["day"] = df["played_at_local"].dt.day
    df["hour_of_day"] = df["played_at_local"].dt.hour
    df["day_of_week"] = df["played_at_local"].dt.day_name()

    logger.info("Transformation complete")
    return df


def upload_transformed(df: pd.DataFrame, date_prefix: str):
    logger.info(f"Uploading transformed dataframe with {len(df)} rows for date_prefix={date_prefix}")
    client = init_minio_client()
    path = f"processed/{date_prefix}/recently_played.parquet"

    try:
        buf = BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        client.put_object(
            MINIO_BUCKET,
            path,
            buf,
            buf.getbuffer().nbytes,
            content_type="application/parquet",
        )
        client.put_object(
            MINIO_BUCKET,
            f"processed/{date_prefix}/_SUCCESS",
            BytesIO(b""),
            0,
            content_type="text/plain",
        )
        logger.info(f"Uploaded parquet and _SUCCESS marker to {path}")
    except Exception:
        logger.error("Failed to upload transformed data", exc_info=True)
        raise