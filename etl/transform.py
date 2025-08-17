import json
import pandas as pd
from io import BytesIO
from minio import Minio
from datetime import datetime, timedelta
from config import MINIO_BUCKET
from utils.minio_utils import init_minio_client

def get_yesterday_date() -> tuple[str, str]:
    """Return yesterday's date in path and string formats."""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y/%m/%d"), yesterday

def download_json_as_df(client: Minio, bucket: str, object_path: str) -> pd.DataFrame:
    """Download a JSON object from MinIO and return as DataFrame."""
    try:
        response = client.get_object(bucket, object_path)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()
        return pd.DataFrame(data)
    except Exception as e:
        raise RuntimeError(f"Failed to download {object_path}: {e}")

def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean the DataFrame."""
    if df.empty:
        raise ValueError("Downloaded dataset is empty! Nothing to transform.")
    df = df.dropna(how="all")
    for col in ["song_title", "artist_name"]:
        if col in df.columns:
            df[col] = df[col].fillna("N/A")
    df = df.dropna(subset=["song_duration_ms", "played_at"], how="any")
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw DataFrame."""
    df = validate_and_clean(df)
    df["song_duration_ms"] = df["song_duration_ms"].astype(int)
    df["played_at"] = pd.to_datetime(df["played_at"], errors="coerce")
    df = df.drop_duplicates(subset=["song_id", "played_at"])
    df["year"] = df["played_at"].dt.year
    df["month"] = df["played_at"].dt.month
    df["hour_of_day"] = df["played_at"].dt.hour
    df["day_of_week"] = df["played_at"].dt.day_name()
    df = df.drop(columns=["played_at"])
    return df

def upload_parquet_to_minio(client: Minio, bucket: str, object_path: str, df: pd.DataFrame):
    """Upload DataFrame as Parquet to MinIO."""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    client.put_object(
        bucket_name=bucket,
        object_name=object_path,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/parquet"
    )
    print(f"Uploaded transformed data to MinIO: s3a://{bucket}/{object_path}")

def write_success_marker(client: Minio, bucket: str, object_path: str):
    """Write a _SUCCESS marker file to MinIO."""
    data = BytesIO(b"")
    client.put_object(
        bucket_name=bucket,
        object_name=object_path,
        data=data,
        length=0,
        content_type="text/plain"
    )
    print(f"Written marker file: s3a://{bucket}/{object_path}")

def success_marker_exists(client: Minio, bucket: str, object_path: str) -> bool:
    """Check if the _SUCCESS marker file exists in MinIO."""
    try:
        client.stat_object(bucket, object_path)
        return True
    except Exception:
        return False

def main():
    minio_client = init_minio_client()
    date_prefix, _ = get_yesterday_date()
    raw_path = f"raw/{date_prefix}/recently_played.json"
    processed_path = f"processed/{date_prefix}/recently_played.parquet"
    marker_path = f"processed/{date_prefix}/_SUCCESS"

    # Check for _SUCCESS marker before processing
    if success_marker_exists(minio_client, MINIO_BUCKET, marker_path):
        print(f"Processed data for {date_prefix} already exists. Skipping transformation.")
        return

    print(f"Reading raw data: {raw_path}")
    df_raw = download_json_as_df(minio_client, MINIO_BUCKET, raw_path)

    print("Validating and transforming data...")
    df_transformed = transform_data(df_raw)

    upload_parquet_to_minio(minio_client, MINIO_BUCKET, processed_path, df_transformed)
    write_success_marker(minio_client, MINIO_BUCKET, marker_path)

if __name__ == "__main__":
    main()