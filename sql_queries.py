# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists song;"
artist_table_drop = "drop table if exists artist;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES


song_table_create = ("""
create table if not exists song (
    song_id varchar(20) PRIMARY KEY NOT NULL,
    title varchar(80) NOT NULL,
    artist_id varchar(20) NOT NULL,
    year int,
    duration NUMERIC(5,2) NOT NULL
);

""")

artist_table_create = ("""
create table if not exists artist (
      artist_id       varchar(25) PRIMARY KEY NOT NULL,
      artist_name     varchar(100) NOT NULL,
      artist_location varchar(35) NOT NULL,
      artist_latitude real NOT NULL,
      artist_longitude real NOT NULL
)
;
""")

time_table_create = ("""
create table if not exists time (
    start_time varchar(25) PRIMARY KEY NOT NULL,
    hour   int NOT NULL,
    day    int NOT NULL,
    week   int NOT NULL,
    month  int NOT NULL,
    year   int NOT NULL,
    weekday int NOT NULL
);
""")

user_table_create = ("""
create table if not exists users (
    user_id int PRIMARY KEY NOT NULL,
    first_name varchar(80) NOT NULL,
    last_name varchar(70) NOT NULL,
    gender char(1) NOT NULL,
    level  char(4) NOT NULL
);
""")

songplay_table_create = ("""

create table if not exists  songplays
(
  songplay_id SERIAL PRIMARY KEY,
  start_time  varchar(25) NOT NULL,
  user_id     int NOT NULL,
  level       char(4) NOT NULL,
  song_id     varchar(25) ,
  artist_id   varchar(25) ,
  session_id  int NOT NULL,
  location    TEXT NOT NULL,
  user_agent  TEXT NOT NULL
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO SONGPLAYS 
  (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) 
VALUES 
  (%s,%s,%s,%s,%s,%s,%s,%s);
""")

user_table_insert = ("""
INSERT INTO USERS 
  (user_id,first_name,last_name,gender,level) 
VALUES 
  (%s,%s,%s,%s,%s) 
ON CONFLICT(user_id) 
  DO UPDATE SET level = EXCLUDED.level;
""")


song_table_insert = ("""
INSERT INTO SONG 
  (song_id,title,artist_id,year,duration) 
VALUES 
  (%s,%s,%s,%s,%s) 
ON CONFLICT (song_id)
  DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO ARTIST 
  (artist_id,artist_name,artist_location,artist_latitude,artist_longitude) 
VALUES 
  (%s,%s,%s,%s,%s) 
ON CONFLICT (artist_id) 
  DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO TIME 
  (start_time,hour,day,week,month,year,weekday) 
VALUES 
  (%s,%s,%s,%s,%s,%s,%s) 
ON CONFLICT(start_time) 
  DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT s.SONG_ID,a.ARTIST_ID 
FROM SONG s join artist a on s.artist_id = a.artist_id 
WHERE UPPER(s.title) = %s and s.duration = %s 
and UPPER(a.artist_name) = %s;
""")


# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
