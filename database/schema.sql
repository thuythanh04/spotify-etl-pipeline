-- Dimensions
CREATE TABLE dim_artist (
    artist_key SERIAL PRIMARY KEY,
    artist_id VARCHAR UNIQUE NOT NULL,
    artist_name VARCHAR NOT NULL
);

CREATE TABLE dim_song (
    song_key SERIAL PRIMARY KEY,
    song_id VARCHAR UNIQUE NOT NULL,
    song_title VARCHAR NOT NULL,
    song_duration_ms INT
);

CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    year INT,
    month INT,
    hour_of_day INT,
    day_of_week VARCHAR,
    CONSTRAINT unique_date_time UNIQUE (
        year,
        month,
        hour_of_day,
        day_of_week
    )
);

-- Fact
CREATE TABLE fact_play_summary (
    play_id SERIAL PRIMARY KEY,
    song_key INT REFERENCES dim_song (song_key),
    artist_key INT REFERENCES dim_artist (artist_key),
    date_key INT REFERENCES dim_date (date_key),
    play_count INT NOT NULL DEFAULT 1,
    total_duration_ms BIGINT NOT NULL,
    CONSTRAINT unique_play_summary UNIQUE (
        song_key,
        artist_key,
        date_key
    )
);