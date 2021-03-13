import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE events_staging (
  artist text
, auth varchar(20)
, firstName text
, gender char(1)
, itemInSession smallint
, lastName text
, length text
, level char(4)
, location text
, method char(10)
, page varchar(20)
, registration bigint
, sessionId smallint
, song text
, status smallint
, ts text
, userAgent text
, userId smallint
)
""")

staging_songs_table_create = ("""
CREATE TABLE SONGS_STAGING (
  num_songs int  
, artist_id varchar(30)
, artist_latitude real 
, artist_longitude real
, artist_location varchar(400)
, artist_name varchar(400)
, song_id varchar
, title text
, duration real
, year int
)
""")

songplay_table_create = ("""
CREATE TABLE SONGPLAYS (
   songplay_id int identity(1, 1) PRIMARY KEY 
 , start_time timestamp not null
 , user_id    smallint  not null
 , level      char(4)   not null
 , song_id    varchar(25) not null
 , artist_id  varchar(25) not null
 , session_id smallint
 , location text
 , user_agent text
)
DISTSTYLE EVEN
""")

user_table_create = ("""
CREATE TABLE users (
  user_id    smallint primary key not null distkey
, first_name varchar(80) not null
, last_name  varchar(70) not null
, gender     char(1)     not null
, level      char(4)     not null
)
""")

song_table_create = ("""
CREATE TABLE songs (
  song_id varchar(20) primary key not null distkey 
, title text not null
, artist_id varchar(20) not null
, year int not null
, duration real not null
)
""")

artist_table_create = ("""
CREATE TABLE artists (
  artist_id varchar(25) primary key not null distkey 
, name varchar(300) not null
, location varchar(400)
, lattitude real
, longitude real
)
""")

time_table_create = ("""
CREATE TABLE time (
  start_time timestamp primary key not null distkey 
, hour int 
, day int 
, week int 
, month int
, year int 
, weekday int 
)
""")

# STAGING TABLES
# print(config.get('S3', 'LOG_DATA'), config.get(
#    'S3', 'LOG_JSONPATH'), config.get('IAM_ROLE', 'ARN'))

staging_events_copy = ("""
COPY events_staging from '{}'
credentials 'aws_iam_role={}'
FORMAT AS JSON '{}' region 'us-west-2'
""").format(config.get('S3', 'LOG_DATA'),  config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

# print(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
COPY SONGS_STAGING FROM '{}'
credentials 'aws_iam_role={}'
FORMAT AS JSON 'auto' region 'us-west-2' 
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO SONGPLAYS 
SELECT DISTINCT 
 timestamp 'epoch' + cast(a.ts AS bigint)/1000 * interval '1 second' AS start_time
,a.userId as user_id 
,a.level
,b.song_id 
,b.artist_id
,a.sessionId as session_id
,a.location
,a.userAgent as user_agent 
FROM events_staging a 
inner join songs_staging b 
  on upper(trim(a.song)) = upper(trim(b.title))
  AND upper(trim(a.artist)) = upper(trim(b.artist_name))
where upper(trim(a.page)) = "NEXTSONG"
""")

user_table_insert = ("""
INSERT INTO users 
select user_id, first_name, last_name, gender, level from 
(
 SELECT   
   userId as user_id
  ,firstName as first_name 
  ,lastName  as last_name 
  ,gender
  ,level 
  ,rank() over(partition by userId order by ts desc ) as rnum
  FROM events_staging 
    where userId is not null 
    and upper(trim(page)) = 'NEXTSONG'
) a
where a.rnum = 1
;
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT 
  song_id 
, title
, artist_id
, year 
, duration 
FROM songs_staging
  WHERE song_id is not null 
""")

artist_table_insert = ("""
INSERT INTO artists 
SELECT DISTINCT 
  artist_id 
, artist_name as name
, artist_location as location 
, artist_lattitude as lattitude
, artist_longitude as longitude
FROM songs_staging 
WHERE artist_id is not null
""")

time_table_insert = ("""
INSERT INTO time
  SELECT DISTINCT 
  timestamp 'epoch' + cast(ts AS bigint)/1000 * interval '1 second' AS start_time
, EXTRACT(hour from start_time)
, EXTRACT(day from start_time)
, EXTRACT(week from start_time)
, EXTRACT(month from start_time)
, EXTRACT(year from start_time)
, EXTRACT(weekday from start_time)
FROM events_staging 
;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# staging_events_copy
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
