def insert_fact_play_summary(cur, song_key, artist_key, date_key, played_at, play_count=1, total_duration_ms=0):
    cur.execute("""
        INSERT INTO fact_play_summary (
            song_key, artist_key, date_key, played_at, play_count, total_duration_ms
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (song_key, artist_key, played_at)
        DO UPDATE
        SET play_count = fact_play_summary.play_count + EXCLUDED.play_count,
            total_duration_ms = fact_play_summary.total_duration_ms + EXCLUDED.total_duration_ms
        RETURNING play_id;
    """, (song_key, artist_key, date_key, played_at, play_count, total_duration_ms))
    return cur.fetchone()[0]