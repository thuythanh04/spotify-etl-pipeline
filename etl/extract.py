import json
import requests
import datetime
from io import BytesIO
from minio import Minio
from config import MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_SECRET_KEY
from etl.utils.spotify_auth import refresh_access_token

def get_access_token() -> str:
    """Refresh and return Spotify access token."""
    return refresh_access_token()

def get_yesterday_timestamp_ms() -> tuple[int, datetime.datetime]:
    """Return yesterday's midnight timestamp in ms and the date object."""
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_midnight = datetime.datetime(
        year=yesterday.year, month=yesterday.month, day=yesterday.day
    )
    return int(yesterday_midnight.timestamp() * 1000), yesterday

def fetch_recently_played(after_ms: int, access_token: str) -> list[dict]:
    """Fetch recently played tracks from Spotify API."""
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"after": after_ms, "limit": 50}
    url = "https://api.spotify.com/v1/me/player/recently-played"
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(f"Spotify API error {response.status_code}: {response.text}")
    data = response.json()
    if "items" not in data:
        raise Exception("Unexpected response format from Spotify API")
    records = []
    for item in data["items"]:
        track = item["track"]
        records.append({
            "song_id": track["id"],
            "song_title": track["name"],
            "artist_name": track["artists"][0]["name"],
            "artist_id": track["artists"][0]["id"],
            "played_at": item["played_at"],
            "song_duration_ms": track["duration_ms"]
        })
    return records

def init_minio_client() -> Minio:
    """Initialize and return a Minio client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def upload_json_to_minio(client: Minio, bucket: str, object_path: str, data: list[dict]):
    """Upload JSON data to MinIO."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    data_bytes = json.dumps(data, indent=2).encode("utf-8")
    data_stream = BytesIO(data_bytes)
    client.put_object(
        bucket_name=bucket,
        object_name=object_path,
        data=data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )
    print(f"Uploaded to MinIO: s3a://{bucket}/{object_path}")

def write_marker(client: Minio, bucket: str, object_path: str):
    """Write an empty marker file to MinIO."""
    client.put_object(
        bucket_name=bucket,
        object_name=object_path,
        data=BytesIO(b""),
        length=0,
        content_type="text/plain"
    )
    print(f"Marker file created: {object_path}")

def marker_exists(client: Minio, bucket: str, object_path: str) -> bool:
    """Check if marker file exists in MinIO."""
    try:
        client.stat_object(bucket, object_path)
        return True
    except Exception:
        return False

def main():
    access_token = get_access_token()
    after_ms, yesterday_date = get_yesterday_timestamp_ms()
    date_prefix = yesterday_date.strftime("%Y/%m/%d")
    minio_client = init_minio_client()
    marker_path = f"raw/{date_prefix}/_SUCCESS"
    if marker_exists(minio_client, MINIO_BUCKET, marker_path):
        print(f"Data for {date_prefix} already exists. Skipping extraction.")
        return
    print(f"Fetching recently played tracks after {after_ms} ({yesterday_date})...")
    records = fetch_recently_played(after_ms, access_token)
    object_name = f"raw/{date_prefix}/recently_played.json"
    upload_json_to_minio(minio_client, MINIO_BUCKET, object_name, records)
    write_marker(minio_client, MINIO_BUCKET, marker_path)

if __name__ == "__main__":
    main()