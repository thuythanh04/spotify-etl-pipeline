-- Dimensions
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_key BIGSERIAL PRIMARY KEY,
    artist_id VARCHAR UNIQUE NOT NULL,
    artist_name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_song (
    song_key BIGSERIAL PRIMARY KEY,
    song_id VARCHAR UNIQUE NOT NULL,
    song_title VARCHAR NOT NULL,
    song_duration_ms BIGINT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key BIGSERIAL PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    hour_of_day INT,
    day_of_week VARCHAR,
    CONSTRAINT unique_date_time UNIQUE (
        year,
        month,
        day,
        hour_of_day,
        day_of_week
    )
);

-- Fact
CREATE TABLE IF NOT EXISTS fact_play_summary (
    play_id BIGSERIAL PRIMARY KEY,
    song_key BIGINT REFERENCES dim_song (song_key),
    artist_key BIGINT REFERENCES dim_artist (artist_key),
    date_key BIGINT REFERENCES dim_date (date_key),
    play_count BIGINT NOT NULL DEFAULT 0,
    total_duration_ms BIGINT NOT NULL DEFAULT 0,
    played_at TIMESTAMPTZ NOT NULL,
    UNIQUE (
        song_key,
        artist_key,
        date_key
    )
);

-- Recommendation
CREATE TABLE IF NOT EXISTS recommendation_list (
    rec_id BIGSERIAL PRIMARY KEY,
    track_name TEXT NOT NULL,
    artist_name TEXT NOT NULL,
    recommended_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    week_of_year INT NOT NULL,
    year INT NOT NULL,
    rank INT NOT NULL,
    UNIQUE (
        track_name,
        artist_name,
        week_of_year,
        year
    )
);