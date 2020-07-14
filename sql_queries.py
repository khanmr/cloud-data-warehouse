import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR(100),
        auth VARCHAR(100),
        first_name VARCHAR(100),
        gender CHAR(1),
        item_in_session INTEGER,
        last_name VARCHAR(100),
        length DOUBLE PRECISION,
        level VARCHAR(100),
        location VARCHAR(100),
        method VARCHAR(10),
        page VARCHAR(50),
        registration BIGINT,
        session_id INTEGER,
        song VARCHAR(255),
        status INTEGER,
        ts BIGINT,
        user_agent VARCHAR(100),
        user_id INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR(255),
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR(100),
        artist_name VARCHAR(100),
        song_id VARCHAR(255),
        title VARCHAR(100),
        duration INTEGER,
        year SMALLINT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id IDENTITY(0, 1) PRIMARY KEY,
        start_time BIGINT NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR(100),
        song_id VARCHAR(255) NOT NULL,
        artist_id VARCHAR(255) NOT NULL,
        session_id INTEGER,
        location VARCHAR(100),
        user_agent VARCHAR(255)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        gender CHAR(1),
        level VARCHAR(50)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(255) PRIMARY KEY,
        title VARCHAR(100),
        artist_id VARCHAR(255),
        year SMALLINT,
        duration INTEGER
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(100),
        location VARCHAR(100),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time BIGINT PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {} 
    CREDENTIALS 'aws_iam_role={}' 
    JSON {}
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    COPY staging_songs FROM {} 
    CREDENTIALS 'aws_iam_role={}' 
    JSON 'auto'
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second',
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    FROM staging_events 
    JOIN staging_songs ON 
        staging_events.artist = staging_songs.artist_name 
        AND staging_events.song = staging_songs.title
        AND staging_events.length = staging_songs.duration
    WHERE page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events 
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
        song_id,
        title,
        artist_id,
        year,
        duration 
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
        (TIMESTAMP 'epoch' + (start_time / 1000) * INTERVAL '1 second') AS tstamp,
        EXTRACT(HOUR FROM tstamp),
        EXTRACT(DAY FROM tstamp),
        EXTRACT(WEEK FROM tstamp),
        EXTRACT(MONTH FROM tstamp),
        EXTRACT(YEAR FROM tstamp),
        EXTRACT(DOW FROM tstamp)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
