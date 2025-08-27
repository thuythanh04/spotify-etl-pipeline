def upsert_artist(cur, artist_id, artist_name):
    cur.execute("""
        INSERT INTO dim_artist (artist_id, artist_name)
        VALUES (%s, %s)
        ON CONFLICT (artist_id) DO UPDATE
        SET artist_name = EXCLUDED.artist_name
        RETURNING artist_key;
    """, (artist_id, artist_name))
    return cur.fetchone()[0]

def upsert_song(cur, song_id, song_title, song_duration_ms):
    cur.execute("""
        INSERT INTO dim_song (song_id, song_title, song_duration_ms)
        VALUES (%s, %s, %s)
        ON CONFLICT (song_id) DO UPDATE
        SET song_title = EXCLUDED.song_title,
            song_duration_ms = EXCLUDED.song_duration_ms
        RETURNING song_key;
    """, (song_id, song_title, song_duration_ms))
    return cur.fetchone()[0]

def upsert_date(cur, year, month, day, hour_of_day, day_of_week):
    cur.execute("""
        INSERT INTO dim_date (year, month, day, hour_of_day, day_of_week)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (year, month, day, hour_of_day, day_of_week) DO NOTHING
        RETURNING date_key;
    """, (year, month, day, hour_of_day, day_of_week))
    result = cur.fetchone()
    if result is not None:
        return result[0]
    # If not inserted, fetch the existing key
    cur.execute("""
        SELECT date_key FROM dim_date
        WHERE year = %s AND month = %s AND day = %s AND hour_of_day = %s AND day_of_week = %s;
    """, (year, month, day, hour_of_day, day_of_week))
    return cur.fetchone()[0]
