import pandas as pd
from io import BytesIO
from minio import Minio
from config import MINIO_BUCKET
from datetime import datetime, timedelta
from utils.fact_loader import insert_fact_play_summary
from utils.dim_loader import upsert_artist, upsert_song, upsert_date
from utils.db import get_connection
from utils.minio_utils import init_minio_client

def get_yesterday_date():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y/%m/%d")

def download_parquet_as_df(client, bucket, object_path):
    response = client.get_object(bucket, object_path)
    df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    response.release_conn()
    return df

def main():
    # Init MinIO + Postgres
    minio_client = init_minio_client()
    conn = get_connection()
    cursor = conn.cursor()

    # Load parquet
    date_prefix = get_yesterday_date()
    parquet_path = f"processed/{date_prefix}/recently_played.parquet"
    print(f"Loading parquet from MinIO: {parquet_path}")
    df = download_parquet_as_df(minio_client, MINIO_BUCKET, parquet_path)

    # Insert into star schema
    for _, row in df.iterrows():
        # dim_artist
        artist_key = upsert_artist(cursor, row["artist_id"], row["artist_name"])

        # dim_song
        song_key = upsert_song(cursor, row["song_id"], row["song_title"], row["song_duration_ms"])

        # dim_date
        date_key = upsert_date(cursor, row["year"], row["month"], row["hour_of_day"], row["day_of_week"])

        # fact_play_history
        if date_key:
            play_id = insert_fact_play_summary(cursor, song_key, artist_key, date_key, 1, row["song_duration_ms"])

    conn.commit()
    cursor.close()
    conn.close()
    print("Load completed!")

if __name__ == "__main__":
    main()