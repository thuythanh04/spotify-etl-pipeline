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
                artist_key = upsert_artist(cursor, row["artist_id"], row["artist_name"])
                song_key = upsert_song(cursor, row["song_id"], row["song_title"], row["song_duration_ms"])

                played_at = pd.to_datetime(row["played_at"])

                # Extract time components for dim_date
                year = played_at.year
                month = played_at.month
                hour_of_day = played_at.hour
                day_of_week = played_at.day_name()

                date_key = upsert_date(cursor, year, month, hour_of_day, day_of_week)

                if date_key:
                    insert_fact_play_summary(
                        cursor,
                        song_key,
                        artist_key,
                        date_key,
                        row["played_at"],
                        1,
                        row["song_duration_ms"],
                    )
                    logger.debug(
                        f"Inserted fact for song_id={row['song_id']}, "
                        f"artist_id={row['artist_id']} at {row['played_at']}"
                    )
            except Exception as e:
                logger.error(f"Failed to process row {idx}", exc_info=True)
                raise

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