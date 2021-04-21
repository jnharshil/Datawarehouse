import configparser
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']


# DROP TABLES

staging_events_table_drop = "DROP table If EXISTS staging_events"
staging_songs_table_drop = "DROP table If EXISTS staging_songs"
songplay_table_drop = "DROP table If EXISTS songs_play_table"
user_table_drop = "DROP table If EXISTS user_table"
artist_table_drop = "DROP table If EXISTS artist_table"
time_table_drop = "DROP table If EXISTS time_table"

# CREATE TABLES
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs ( staging_song_id BIGINT IDENTITY(0,1) PRIMARY KEY, num_songs INT, artist_id VARCHAR(30),artist_latitude NUMERIC,artist_longitude NUMERIC,artist_location VARCHAR(500),artist_name VARCHAR(200),song_id VARCHAR(30),title VARCHAR(500),duration NUMERIC,year INT) """)

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (staging_event_id BIGINT IDENTITY(0,1) PRIMARY KEY,artist VARCHAR ,auth VARCHAR ,firstname VARCHAR ,gender VARCHAR ,itemInSession INT ,lastName VARCHAR ,length VARCHAR,level VARCHAR,location VARCHAR,method VARCHAR,page VARCHAR,registration BIGINT,sessionId VARCHAR,song VARCHAR,status INT,ts VARCHAR,userAgent VARCHAR,userId BIGINT )""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songs_play_table ( start_time TIMESTAMP, user_id int, level VARCHAR, song_id VARCHAR, artist_id VARCHAR, session_id VARCHAR, location VARCHAR, user_agent VARCHAR)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table (user_id INT PRIMARY KEY NOT NULL, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table (song_id VARCHAR PRIMARY KEY NOT NULL , title VARCHAR, artist_id VARCHAR, year INT, duration INT)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table (artist_id VARCHAR PRIMARY KEY NOT NULL , name VARCHAR, location VARCHAR, lattitude VARCHAR, longitude VARCHAR)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table (start_time VARCHAR , hour VARCHAR, day VARCHAR, week VARCHAR, month VARCHAR, year VARCHAR, weekday VARCHAR)
""")

# # STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json {};
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto'
""").format(SONG_DATA, IAM_ROLE)
# FINAL TABLES

songplay_table_insert =("""INSERT INTO songs_play_table(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,se.userId,se.level,ss.song_id,ss.artist_id,se.sessionId,se.location, se.userAgent
FROM staging_songs ss
INNER JOIN staging_events se
ON (ss.title = se.song AND se.artist = ss.artist_name)
AND se.page = 'NextSong';""")

user_table_insert = ("""INSERT INTO user_table (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userId)  AS user_id, firstName AS first_name,lastName AS last_name, gender,level FROM staging_events WHERE user_id IS NOT NULL AND page = 'NextSong';
""")
song_table_insert = """INSERT INTO song_table(song_id,title,artist_id,year,duration ) SELECT DISTINCT(song_id)  AS song_id, title AS title,artist_id AS artist_id, year,duration FROM staging_songs WHERE song_id IS NOT NULL 
"""

artist_table_insert = """INSERT INTO artist_table (artist_id  , name , location , lattitude , longitude ) SELECT DISTINCT(artist_id)  AS artist_id, artist_name AS name,artist_location AS location, artist_latitude AS lattitude,artist_longitude AS longitude FROM staging_songs WHERE artist_id IS NOT NULL """

time_table_insert = ("""INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
SELECT a.start_time,
EXTRACT (HOUR FROM a.start_time), EXTRACT (DAY FROM a.start_time),
EXTRACT (WEEK FROM a.start_time), EXTRACT (MONTH FROM a.start_time),
EXTRACT (YEAR FROM a.start_time), EXTRACT (WEEKDAY FROM a.start_time) FROM
(SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_events) a;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create ,user_table_create ,song_table_create ,artist_table_create ,time_table_create]
drop_table_queries = [ songplay_table_drop, user_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
