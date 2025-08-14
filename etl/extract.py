import json
import requests
import boto3
import time
from pathlib import Path
from spotify_auth import refresh_access_token
from datetime import datetime
from config import MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_SECRET_KEY

# Initialize MinIO client (S3-compatible)
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Ensure bucket exists
try:
    s3_client.head_bucket(Bucket=MINIO_BUCKET)
except:
    s3_client.create_bucket(Bucket=MINIO_BUCKET)

ACCESS_TOKEN = refresh_access_token()
BASE_URL = "https://api.spotify.com/v1"

def make_request(url, params=None):
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    r = requests.get(url, headers=headers, params=params)
    if r.status_code != 200:
        raise Exception(f"Error {r.status_code}: {r.text}")
    return r.json()

def save_json_minio(data, key):
    """Save JSON to MinIO at a given key"""
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    print(f"Saved to MinIO: {key}")

def load_json_minio(key):
    """Load JSON from MinIO"""
    try:
        obj = s3_client.get_object(Bucket=MINIO_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        return None

# API Calls
def get_followed_artists():
    url = f"{BASE_URL}/me/following"
    params = {"type": "artist", "limit": 50}
    return make_request(url, params)

def get_recently_played_incremental(after_ms):
    url = f"{BASE_URL}/me/player/recently-played"
    params = {"after": after_ms, "limit": 50}
    return make_request(url, params)

def get_saved_tracks(limit=50):
    url = f"{BASE_URL}/me/tracks"
    params = {"limit": limit}
    return make_request(url, params)


if __name__ == "__main__":
    print("Extracting data from Spotify...")

    # Load last timestamp from metadata in MinIO
    metadata_key = "metadata/spotify_incremental.json"
    metadata = load_json_minio(metadata_key)
    if metadata and "last_timestamp_ms" in metadata:
        after_ms = metadata["last_timestamp_ms"]
        print(f"Resuming from last timestamp: {after_ms}")
    else:
        after_ms = int((time.time() - 24*60*60) * 1000)  # default 24h ago
        print("No metadata found. Starting from last 24h.")
        

    # Fetch followed artists
    followed_artists = get_followed_artists()
    date_prefix = datetime.utcnow().strftime("%Y/%m/%d")
    save_json_minio(followed_artists, f"raw/{date_prefix}/followed_artists.json")

    # Fetch incremental recently played tracks
    timestamp_str = datetime.utcnow().strftime("%H%M%S")
    recently_played = get_recently_played_incremental(after_ms)
    save_json_minio(recently_played, f"raw/{date_prefix}/{timestamp_str}_recently_played.json")

    track_items = recently_played.get("items", [])
    if track_items:
        new_last_timestamp_ms = max(item["played_at"] for item in track_items)
        new_last_timestamp_ms = int(datetime.fromisoformat(new_last_timestamp_ms.replace("Z", "+00:00")).timestamp() * 1000)

        # Update metadata in MinIO
        new_metadata = {"last_timestamp_ms": new_last_timestamp_ms}
        save_json_minio(new_metadata, metadata_key)

    # Fetch saved tracks
    saved_tracks = get_saved_tracks()
    save_json_minio(saved_tracks, f"raw/{date_prefix}/saved_tracks.json")

    # Write logs in MinIO
    log_entry = {
        "run_time_utc": datetime.utcnow().isoformat(),
        "recently_played_count": len(track_items),
        "saved_tracks_count": len(saved_tracks.get("items", []))
    }
    log_key = f"logs/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    save_json_minio(log_entry, log_key)

    print("Extraction completed!")
