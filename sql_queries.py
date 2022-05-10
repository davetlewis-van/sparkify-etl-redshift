import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
    artist_id VARCHAR,
    artist_latitude DOUBLE PRECISION,
    artist_location VARCHAR,
    artist_longitude DOUBLE PRECISION,
    artist_name VARCHAR,
    duration FLOAT,
    num_songs INT,
    song_id VARCHAR,
    title VARCHAR,
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL,
    user_id BIGINT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR,
    PRIMARY KEY(songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
    user_id BIGINT,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR,
    PRIMARY KEY(user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
    song_id VARCHAR,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration FLOAT NOT NULL,
    PRIMARY KEY(song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
    artist_id VARCHAR,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY(artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
    start_time TIMESTAMP,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT,
    PRIMARY KEY(start_time)
);
""")

# STAGING TABLES
# Do the necessary timestamp conversion in the COPY command
# https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat
staging_events_copy = ("""
    COPY staging_events FROM '{}'
    credentials 'aws_iam_role={}'
    json '{}' 
    region 'us-west-2'
    TIMEFORMAT 'epochmillisecs';
""").format(config['S3']['LOG_DATA'], 
            config['IAM_ROLE']['ARN'], 
            config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs FROM '{}'
    credentials 'aws_iam_role={}'
    json 'auto ignorecase' 
    region 'us-west-2';
""").format(config['S3']['SONG_DATA'], 
            config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT
    e.ts AS start_time,
    e.userId AS user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId AS session_id,
    e.location,
    e.userAgent AS user_agent
FROM staging_events e
LEFT JOIN staging_songs s ON (e.song = s.title 
    AND e.artist = s.artist_name
    AND e.length = s.duration) 
WHERE e.page = 'NextSong'
;
"""

user_table_insert = """
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT 
    userId AS user_id,
    firstName AS first_name,
    lastName as last_name,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong'
;
"""

song_table_insert = """
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT 
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
;
"""

artist_table_insert = """
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT
    artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs
;
"""

time_table_insert = """
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT 
    ts AS start_time,
    EXTRACT(HOUR FROM ts) AS hour,
    EXTRACT(DAY FROM ts) AS day,
    EXTRACT(WEEK FROM ts) AS week,
    EXTRACT(MONTH FROM ts) AS month,
    EXTRACT(YEAR FROM ts) AS year,
    EXTRACT(DOW FROM ts) AS weekday
FROM staging_events
WHERE page = 'NextSong'
;
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
