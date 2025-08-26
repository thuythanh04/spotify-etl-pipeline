-- Total Songs Explored
SELECT COUNT(DISTINCT song_key) AS total_songs FROM dim_song;

-- Total Artists Explored
SELECT COUNT(DISTINCT artist_key) AS total_artists FROM dim_artist;

-- Total Listening Hours
SELECT ROUND(
        SUM(total_duration_ms) / 1000 / 60 / 60, 2
    ) AS total_listening_hours
FROM fact_play_summary;

-- Weekend vs Weekday Listening Trend
SELECT
    CASE
        WHEN d.day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    d.hour_of_day,
    SUM(f.play_count) AS total_plays,
    SUM(f.total_duration_ms) / 60000 AS total_minutes
FROM
    fact_play_summary f
    JOIN dim_date d ON f.date_key = d.date_key
GROUP BY
    day_type,
    d.hour_of_day
ORDER BY day_type, d.hour_of_day;

-- Top 5 Most Played Artists
SELECT a.artist_name, SUM(f.play_count) AS total_plays
FROM
    fact_play_summary f
    JOIN dim_artist a ON f.artist_key = a.artist_key
GROUP BY
    a.artist_name
ORDER BY total_plays DESC
LIMIT 5;

-- Top 10 most played songs
SELECT s.song_title, d.hour_of_day, SUM(f.play_count) AS total_play_count
FROM
    fact_play_summary f
    JOIN dim_song s ON f.song_key = s.song_key
    JOIN dim_date d ON f.date_key = d.date_key
GROUP BY
    s.song_title,
    d.hour_of_day
ORDER BY total_play_count DESC
LIMIT 10;

-- Play Duration for Each Hour
SELECT d.hour_of_day, ROUND(
        SUM(f.total_duration_ms) / 60000, 2
    ) AS total_minutes
FROM
    fact_play_summary f
    JOIN dim_date d ON f.date_key = d.date_key
GROUP BY
    d.hour_of_day
ORDER BY d.hour_of_day;