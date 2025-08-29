import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime
from scipy.spatial.distance import cdist
import psycopg2
from datetime import datetime
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

# Load and preprocess features
def load_spotify_features(path: str) -> pd.DataFrame:
    """Load Spotify features, one-hot encode categoricals, normalize numericals, and drop irrelevant columns."""
    df = pd.read_csv(path)

    # One-hot encode categorical features
    categorical = ["genre", "key"]
    df = pd.get_dummies(df, columns=categorical)

    # Normalize numerical features
    scaler = MinMaxScaler()
    numerical = df.select_dtypes(np.number).columns.tolist()
    df[numerical] = scaler.fit_transform(df[numerical])

    # Drop irrelevant columns if present
    drop_cols = ["popularity", "mode", "time_signature"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    return df

# Database fetch
def get_recently_played() -> pd.DataFrame:
    """Fetch recently played songs from Postgres fact + dimension tables."""
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host="localhost",
        port="15432"
    )
    query = """
        SELECT s.song_id, s.song_title, a.artist_name, a.artist_id, 
               s.song_duration_ms, fp.played_at
        FROM fact_play_summary fp
        JOIN dim_song s   ON fp.song_key = s.song_key
        JOIN dim_artist a ON fp.artist_key = a.artist_key
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Playlist construction
def generate_playlist_df(recent_df: pd.DataFrame, spotify_features: pd.DataFrame) -> pd.DataFrame:
    """Merge recent plays with Spotify features by song + artist."""
    playlist_df = recent_df.merge(
        spotify_features,
        left_on=["song_title", "artist_name"],
        right_on=["track_name", "artist_name"],
        how="inner",
    )

    playlist_df["played_at"] = pd.to_datetime(playlist_df["played_at"], utc=True)
    return playlist_df.sort_values("played_at", ascending=False)


# Playlist vector
def generate_playlist_vector(playlist_df: pd.DataFrame, spotify_features: pd.DataFrame):
    """Compute recency-weighted playlist vector and candidate pool of non-playlist songs."""
    now = pd.Timestamp.now(tz="UTC")

    # Exponential decay weights by recency
    playlist_df["weight"] = np.exp(
        -(now - playlist_df["played_at"]).dt.total_seconds() / (3600 * 24)
    )

    # Match songs with features
    playlist_features = spotify_features.merge(
        playlist_df[["song_title", "artist_name", "weight"]],
        left_on=["track_name", "artist_name"],
        right_on=["song_title", "artist_name"],
        how="inner",
    )

    if playlist_features.empty or playlist_features["weight"].sum() == 0:
        raise ValueError("Playlist is empty or weights could not be computed.")

    # Extract weighted feature matrix
    weights = playlist_features["weight"].values
    feature_matrix = playlist_features.drop(
        columns=["track_id", "track_name", "artist_name", "song_title", "weight"]
    ).values

    # Weighted playlist vector
    weighted_vector = np.dot(weights.T, feature_matrix) / weights.sum()

    # Candidate songs = all features not in playlist
    nonplaylist_df = (
        spotify_features.merge(
            playlist_df[["song_title", "artist_name"]],
            left_on=["track_name", "artist_name"],
            right_on=["song_title", "artist_name"],
            how="left",
            indicator=True,
        )
        .query('_merge == "left_only"')
        .drop(columns=["_merge", "song_title"])
    )

    return weighted_vector, nonplaylist_df

# Recommendations
def generate_recommendations(playlist_vector: np.ndarray, nonplaylist_df: pd.DataFrame, top_n: int = 15):
    """Compute cosine similarity and return top-N recommended songs."""
    nonplaylist_features = nonplaylist_df.drop(
        columns=["track_id", "track_name", "artist_name"]
    ).values
    nonplaylist_meta = nonplaylist_df[["track_name", "artist_name"]].values

    # Compute cosine distance
    distances = cdist(nonplaylist_features, playlist_vector, metric="cosine").flatten()
    rec_indices = distances.argsort()[:top_n]

    return nonplaylist_meta[rec_indices]

def save_recommendations(recommendations, conn):
    now = datetime.utcnow()
    week = now.isocalendar()[1]
    year = now.year
    saved, skipped = 0, 0

    with conn.cursor() as cur:
        for rank, (track, artist) in enumerate(recommendations, start=1):
            cur.execute("""
                INSERT INTO fact_recommendation (
                    track_name, artist_name, recommended_at,
                    week_of_year, year, rank
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (track_name, artist_name, week_of_year, year)
                DO NOTHING
                RETURNING rec_id;
            """, (track, artist, now, week, year, rank))

            if cur.fetchone():
                saved += 1
            else:
                skipped += 1

    conn.commit()
    print(f"✅ Saved {saved} recommendations, skipped {skipped} (duplicates).")
    
if __name__ == "__main__":
    spotify_features = load_spotify_features("data/SpotifyFeatures.csv")
    recent_df = get_recently_played()
    playlist_df = generate_playlist_df(recent_df, spotify_features)
    playlist_vector, nonplaylist_df = generate_playlist_vector(playlist_df, spotify_features)

    recommendations = generate_recommendations(playlist_vector.reshape(1, -1), nonplaylist_df)

    print("Recommended tracks:")
    for track, artist in recommendations:
        print(f"{track} — {artist}")

    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host="localhost",
        port="15432"
    )
    save_recommendations(recommendations, conn)
    conn.close()