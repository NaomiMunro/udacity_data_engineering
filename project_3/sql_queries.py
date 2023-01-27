import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config['S3']['LOG_DATA']
ARN = config['IAM_ROLE']['ARN']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

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
        artist        varchar,
        auth          varchar,
        firstName     varchar,
        gender        varchar,
        itemInSession int,
        lastName      varchar,
        length        float,
        level         varchar,
        location      varchar,
        method        varchar,
        page          varchar,
        registration  float,
        sessionId     int,
        song          varchar,
        status        int,
        ts            bigint,
        userAgent     varchar,
        userId        int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id        varchar,
        artist_latitude  float,
        artist_location  varchar,
        artist_longitude float,
        artist_name      varchar,
        duration         float,
        num_songs        int,
        song_id          varchar,
        title            varchar,
        year             int
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id  bigint identity(0,1) PRIMARY KEY,
        start_time   timestamp not null,
        user_id      int not null,
        level        varchar,
        song_id      varchar,
        artist_id    varchar,
        session_id   int,
        location     varchar,
        user_agent   varchar
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     int PRIMARY KEY,
        first_name  varchar,
        last_name   varchar,
        gender      varchar,
        level       varchar
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id    varchar PRIMARY KEY,
        title      varchar not null,
        artist_id  varchar,
        year       int,
        duration   float not null
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id  varchar PRIMARY KEY,
        name       varchar not null,
        location   varchar,
        latitude   float,
        longitude  float
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  timestamp PRIMARY KEY,
        hour        int,
        day         int,
        week        int,
        month       int,
        year        int,
        weekday     int
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    json '{}'
    region 'us-west-2'
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second',
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events e
    JOIN staging_songs s
    ON s.artist_name = e.artist AND s.title = e.song
    WHERE e.page = 'NextSong' AND e.userId IS NOT NULL;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time,
        EXTRACT(HOUR FROM start_time), 
        EXTRACT(DAY FROM start_time),
        EXTRACT(WEEK FROM start_time),
        EXTRACT(MONTH FROM start_time),
        EXTRACT(YEAR FROM start_time),
        EXTRACT(DOW FROM start_time)
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
