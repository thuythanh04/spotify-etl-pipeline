import logging
import json, datetime, requests
from io import BytesIO
from minio import Minio
from config import MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_SECRET_KEY
from etl.utils.spotify_auth import refresh_access_token

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

def get_yesterday_timestamp_ms() -> tuple[int, datetime.datetime]:
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_midnight = datetime.datetime(year=yesterday.year, month=yesterday.month, day=yesterday.day)
    logging.info(f"Computed yesterday's timestamp: {yesterday_midnight}")
    return int(yesterday_midnight.timestamp() * 1000), yesterday

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

def init_minio_client() -> Minio:
    logging.info(f"Initializing MinIO client to endpoint {MINIO_ENDPOINT}")
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

def upload_raw(records: list[dict], yesterday_date: datetime.datetime):
    client = init_minio_client()
    date_prefix = yesterday_date.strftime("%Y/%m/%d")
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