"""
SQL statements to create tables, insert data into them and 
drop them from the database.
"""


# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

songplay_table_create = """
CREATE TABLE IF NOT EXISTS fact_songplay (
    songplay_id SERIAL, 
    user_id int NOT NULL, 
    song_id varchar, 
    artist_id varchar, 
    session_id int NOT NULL, 
    start_time timestamp NOT NULL, 
    level varchar NOT NULL, 
    location varchar, 
    user_agent varchar,
    PRIMARY KEY (songplay_id));
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS dim_user (
    user_id int, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar,
    PRIMARY KEY (user_id, level)
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS dim_song (
    song_id varchar, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int, 
    duration numeric CHECK (duration > 0),
    PRIMARY KEY (song_id)
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id varchar, 
    name varchar NOT NULL, 
    location varchar, 
    latitude numeric, 
    longitude numeric,
    PRIMARY KEY (artist_id)
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS dim_time (
    start_time timestamp, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday varchar,
    PRIMARY KEY (start_time)
);
"""

# INSERT RECORDS

songplay_table_insert = """
INSERT INTO fact_songplay (
    user_id, 
    song_id, 
    artist_id, 
    session_id, 
    start_time,
    level, 
    location, 
    user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

user_table_insert = """
INSERT INTO dim_user (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id, level) 
DO
    UPDATE
    SET level = EXCLUDED.level;
"""

song_table_insert = """
INSERT INTO dim_song (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
"""

artist_table_insert = """
INSERT INTO dim_artist (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
"""

time_table_insert = """
INSERT INTO dim_time (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
"""

# FIND SONGS

song_select = """
SELECT s.song_id, a.artist_id FROM dim_song s
JOIN dim_artist a ON s.artist_id = a.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration = %s;
"""

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
