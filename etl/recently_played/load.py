import pandas as pd
from io import BytesIO
import logging
from etl.utils.db import get_connection
from etl.utils.fact_loader import insert_fact_play_summary
from etl.utils.dim_loader import upsert_artist, upsert_song, upsert_date
from etl.utils.minio_utils import init_minio_client
from config import MINIO_BUCKET

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_int(value, default=0):
    """Convert value to int safely, return default if NaN."""
    return int(value) if pd.notna(value) else default

def download_processed(date_prefix: str) -> pd.DataFrame:
    logger.info(f"Downloading processed data for date_prefix={date_prefix}")
    client = init_minio_client()
    path = f"processed/{date_prefix}/recently_played.parquet"

    try:
        response = client.get_object(MINIO_BUCKET, path)
        df = pd.read_parquet(BytesIO(response.read()))
        logger.info(f"Downloaded {len(df)} rows from {path}")
    except Exception as e:
        logger.error(f"Failed to download processed data from {path}", exc_info=True)
        raise
    finally:
        response.close()
        response.release_conn()

    return df

def load_to_postgres(df: pd.DataFrame):
    logger.info(f"Loading dataframe with {len(df)} rows into Postgres")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        for idx, row in df.iterrows():
            logger.debug(f"Processing row {idx}: {row.to_dict()}")

            try:
                # Skip rows with missing critical values
                if pd.isna(row.get("played_at")) or pd.isna(row.get("artist_id")) or pd.isna(row.get("song_id")):
                    logger.warning(f"Skipping row {idx} due to missing critical values: {row.to_dict()}")
                    continue

                # Ensure string IDs and names
                artist_id = str(row["artist_id"])
                song_id = str(row["song_id"])
                artist_name = str(row["artist_name"])
                song_title = str(row["song_title"])

                # Safe numeric conversions
                song_duration_ms = safe_int(row.get("song_duration_ms"), default=0)
                play_count = safe_int(row.get("play_count"), default=1)

                # Upsert dimension tables
                artist_key = upsert_artist(cursor, artist_id, artist_name)
                song_key = upsert_song(cursor, song_id, song_title, song_duration_ms)

                # Convert played_at to datetime
                played_at = pd.to_datetime(row["played_at"])

                # Extract date components safely
                year = safe_int(played_at.year)
                month = safe_int(played_at.month)
                hour_of_day = safe_int(played_at.hour)
                day_of_week = played_at.day_name() if pd.notna(played_at) else "Unknown"

                date_key = upsert_date(cursor, year, month, hour_of_day, day_of_week)

                if date_key:
                    insert_fact_play_summary(
                        cursor,
                        song_key,
                        artist_key,
                        date_key,
                        played_at,
                        play_count=play_count,
                        total_duration_ms=song_duration_ms,
                    )

                    logger.debug(
                        f"Inserted fact for song_id={song_id}, artist_id={artist_id} at {played_at}"
                    )

            except Exception as e:
                logger.warning(f"Skipping row {idx} due to processing error", exc_info=True)
                # Skip row and continue
                continue

        conn.commit()
        logger.info("Postgres load committed successfully")

    except Exception as e:
        conn.rollback()
        logger.error("Error during Postgres load, rolled back transaction", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()
        logger.info("Postgres connection closed")