import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table if exists staging_events "
staging_songs_table_drop = "DROP table if exists staging_songs "
songplay_table_drop = "DROP table if exists songplays "
user_table_drop = "DROP table if exists users "
song_table_drop = "DROP table if exists songs "
artist_table_drop = "DROP table if exists artists "
time_table_drop = "DROP table if exists time "

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
artist text, auth text, firstName text, gender text, ItemInSession int,
lastName text, length float8, level text, location text, method text,
page text, registration text, sessionId int, song text, status int,
ts bigint , userAgent text, userId int)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
song_id text PRIMARY KEY, artist_id text, artist_latitude float,
artist_longitude float, artist_location text, artist_name text,
duration float, num_songs int, title text, year int)
""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY, first_name text, last_name text, gender text, level text)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
song_id  text PRIMARY KEY, title text, artist_id text, year int, duration float)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
artist_id text PRIMARY KEY, name text, location text, lattitude text, longitude text)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY
                     ,hour varchar
                     ,day int
                     ,week int
                     ,month int
                     ,year int
                     ,weekday int)
""")

# def create_table_queries():
# CREATE TABLES
#songplay_id, start_time, user-id, level, song_id, artist_id, sessions_id, location, user_agent
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
songplay_id serial NOT NULL PRIMARY KEY, start_time TIMESTAMP , user_id int, level text, song_id text, artist_id text, session_id int, location text, user_agent text)
""")

# STAGING TABLES

###################
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")
ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

###################3

staging_events_copy = ("""
copy staging_events from '{}'
credentials 'aws_iam_role={}'
json '{}' compupdate on region 'us-west-2';
""").format(LOG_DATA,ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from '{}'
credentials 'aws_iam_role={}'
json 'auto' compupdate on region 'us-west-2';
""").format(SONG_DATA, ARN)


# FINAL TABLES

songplay_table_insert = ("""
insert into songplays
(start_time,user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
DISTINCT timestamp 'epoch' + se.ts * interval '0.001 seconds' as start_time,
se.userId,
se.level,
ss.song_id,
ss.artist_id,
se.sessionId,
se.location,
se.userAgent
FROM staging_events se, staging_songs ss
WHERE se.page = 'NextSong'
AND se.song = ss.title
AND se.artist = ss.artist_name
AND se.length = ss.duration
and se.userId is not null
""")


user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT  
  DISTINCT(userId)  AS user_id,
  firstName     AS first_name,
  lastName     AS last_name,
  gender,
  level
FROM 
  staging_events
WHERE 
  user_id IS NOT NULL
  AND page  =  'NextSong';
""")

    
song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration) 
    SELECT song_id,title,artist_id,year,duration FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, lattitude, longitude) 
    SELECT artist_id,artist_name,artist_location,artist_latitude,artist_longitude FROM staging_songs;
""")


time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday)
                SELECT  TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time, extract(hour from start_time),
                     extract(day from start_time),extract(week from start_time)
                     ,extract(month from start_time),extract(year from start_time)
                     ,extract(weekday from start_time) from staging_events;
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,user_table_create, artist_table_create,time_table_create, songplay_table_create, song_table_create ]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
