import json
import requests
from spotify_auth import refresh_access_token
import datetime
from config import MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_SECRET_KEY
from minio import Minio
from io import BytesIO
import io

ACCESS_TOKEN = refresh_access_token()

def get_yesterday_timestamp_ms():
    """Get yesterday's start time in milliseconds (for Spotify API)."""
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_midnight = datetime.datetime(
        year=yesterday.year, month=yesterday.month, day=yesterday.day
    )
    return int(yesterday_midnight.timestamp() * 1000), yesterday

def fetch_recently_played(after_ms):
    """Fetch recently played tracks from Spotify API."""
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }
    params = {
        "after": after_ms,
        "limit": 50
    }
    url = "https://api.spotify.com/v1/me/player/recently-played"
    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        raise Exception(f"Spotify API error {response.status_code}: {response.text}")
    
    data = response.json()
    
    if "items" not in data:
        raise Exception("Unexpected response format from Spotify API")
    
    records = []
    
    # Extract track data
    for item in data["items"]:
        track = item["track"]
        record = {
            "song_id": track["id"],
            "song_title": track["name"],
            "artist_name": track["artists"][0]["name"],
            "artist_id": track["artists"][0]["id"],
            "played_at": item["played_at"],
            "song_duration_ms": track["duration_ms"]
        }
        records.append(record)
    
    return records

def upload_to_minio(client, bucket_name, object_path, data):
    """Upload JSON data to MinIO."""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    data_bytes = json.dumps(data, indent=2).encode("utf-8")
    data_stream = BytesIO(data_bytes)

    client.put_object(
        bucket_name=bucket_name,
        object_name=object_path,
        data=data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )
    print(f"Uploaded to MinIO: s3a://{bucket_name}/{object_path}")
    
def write_marker(minio_client, bucket_name, object_name):
    # Empty file as a success flag
    marker_data = b""
    minio_client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(marker_data),
        length=0
    )
    print(f"Marker file created: {object_name}")

def marker_exists(minio_client, bucket_name, object_name):
    try:
        minio_client.stat_object(bucket_name, object_name)
        return True
    except Exception:
        return False

if __name__ == "__main__":
    # Get yesterday timestamp & date
    after_ms, yesterday_date = get_yesterday_timestamp_ms()
    date_prefix = yesterday_date.strftime("%Y/%m/%d")

    # MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    marker_path = f"raw/{date_prefix}/_SUCCESS"
    if marker_exists(minio_client, MINIO_BUCKET, marker_path):
        print(f"Data for {date_prefix} already exists. Skipping extraction.")
    else:
        # Fetch from Spotify
        print(f"Fetching recently played tracks after {after_ms} ({yesterday_date})...")
        records = fetch_recently_played(after_ms)

        # Upload JSON
        object_name = f"raw/{date_prefix}/recently_played.json"
        upload_to_minio(minio_client, MINIO_BUCKET, object_name, records)

        # Upload marker
        write_marker(minio_client, MINIO_BUCKET, marker_path)