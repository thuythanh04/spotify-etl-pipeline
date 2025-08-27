import logging
import json, datetime, requests
from datetime import datetime, timedelta, timezone
from io import BytesIO
from config import MINIO_BUCKET
from etl.utils.spotify_auth import refresh_access_token
from etl.utils.minio_utils import init_minio_client

# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------- Helpers ----------
def get_access_token() -> str:
    token = refresh_access_token()
    logging.info("Spotify access token retrieved")
    return token

def get_last_window_timestamp_ms(hours: int = 12) -> tuple[int, datetime]:
    """
    Returns the timestamp (ms) for the start of the last ETL window,
    based on the given interval in hours.
    """

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=hours)

    logging.info(f"Computed ETL window start: {window_start}")
    return int(window_start.timestamp() * 1000), window_start


def fetch_recently_played(after_ms: int, access_token: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"after": after_ms, "limit": 50}
    url = "https://api.spotify.com/v1/me/player/recently-played"
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    items = data.get("items", [])
    logging.info(f"Fetched {len(items)} recently played tracks from Spotify")
    return [
        {
            "song_id": item["track"]["id"],
            "song_title": item["track"]["name"],
            "artist_name": item["track"]["artists"][0]["name"],
            "artist_id": item["track"]["artists"][0]["id"],
            "played_at": item["played_at"],
            "song_duration_ms": item["track"]["duration_ms"],
        }
        for item in items
    ]

def upload_raw(records: list[dict], yesterday_date):
    client = init_minio_client()

    if isinstance(yesterday_date, str):
        # If string, assume it's already like "2024/08/22"
        date_prefix = yesterday_date
    else:
        # If datetime, format it
        date_prefix = yesterday_date.strftime("%Y-%m-%d-%H")

    path = f"raw/{date_prefix}/recently_played.json"
    data_bytes = json.dumps(records, indent=2).encode("utf-8")
    client.put_object(MINIO_BUCKET, path, BytesIO(data_bytes), length=len(data_bytes), content_type="application/json")
    logging.info(f"Uploaded {len(records)} records to MinIO at {path}")
    return date_prefix


def write_success_marker(bucket: str, date_prefix: str):
    client = init_minio_client()
    path = f"raw/{date_prefix}/_SUCCESS"
    client.put_object(bucket, path, BytesIO(b""), 0, content_type="text/plain")
    logging.info(f"Success marker written to {bucket}/{path}")