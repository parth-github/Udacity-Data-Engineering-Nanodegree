# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR(10),
    song_id TEXT,
    artist_id TEXT,
    session_id INTEGER NOT NULL,
    location VARCHAR(50),
    user_agent VARCHAR(150)
);
""")

user_table_create = ("""
create table if not exists users (
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR(20),
    last_name VARCHAR(20), 
    gender CHAR(1), 
    level VARCHAR(10)
   
);
""")

song_table_create = ("""
create table if not exists songs (
    song_id TEXT PRIMARY KEY, 
    title TEXT NOT NULL, 
    artist_id TEXT NOT NULL, 
    year INTEGER, 
    duration  FLOAT NOT NULL
);
""")

artist_table_create = ("""
create table if not exists artists (
    artist_id TEXT PRIMARY KEY, 
    name TEXT NOT NULL, 
    location TEXT, 
    latitude FLOAT, 
    longitude FLOAT
);
""")

time_table_create = ("""
create table if not exists time (
    start_time TIMESTAMP PRIMARY KEY, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER
);
""")

# INSERT RECORDS
# not require to separately add songplay_id, since you are using type SERIAL i.e. an auto increment type for songplay_id. This basically, add a new row with updated index value for songplay_id column.
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(songplay_id) DO NOTHING;
""")

'''
For the user table insert query, the conflict resolution is a little different. A user will be present even if he/she is a free tier user. But what if the free tier user converts into a paid user. In that case we have to modify the level of the user as below:
ON CONFLICT(user_id) DO UPDATE SET level = excluded.level
'''
user_table_insert = ("""
INSERT INTO users ("user_id","first_name", "last_name", "gender", "level")
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(user_id) DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
insert into songs ("song_id", "title", "artist_id", "year", "duration")
values (%s,%s,%s,%s,%s)
ON CONFLICT(song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists ("artist_id", "name", "location","latitude","longitude")
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT(artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time ("start_time", "hour", "day","week","month","year","weekday")
VALUES (%s, %s, %s, %s, %s, %s, %s) 
ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT ss.song_id, ss.artist_id 
FROM songs ss 
JOIN artists ars 
    on ss.artist_id = ars.artist_id
WHERE ss.title = %s
AND ars.name = %s
AND ss.duration = %s
;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]