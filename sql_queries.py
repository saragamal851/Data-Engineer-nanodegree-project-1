# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists  users;"
song_table_drop = "drop table if exists  songs;"
artist_table_drop = "drop table if exists  artists;"
time_table_drop = "drop table if exists  time;"
songplaytemp_table_drop="drop table if exists  songplaytemp;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
  (
     songplay_id serial primary key,
     start_time  TIMESTAMP ,
     userid      INT ,
     level       TEXT,
     song_id     TEXT ,
     artist_id   TEXT ,
     sessionid   INT,
     location    TEXT,
     useragent   TEXT
  );  """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
  (
     userid    INT primary key,
     firstname TEXT,
     lastname  TEXT,
     gender    TEXT,
     level     TEXT
  );  """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
  (
     song_id   TEXT primary key,
     title     TEXT,
     artist_id TEXT,
     year      INT,
     duration  DECIMAL
  );  """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
  (
     artist_id        TEXT primary key,
     artist_name      TEXT,
     artist_location  TEXT,
     artist_latitude  FLOAT,
     artist_longitude FLOAT
  );  """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
  (
     start_time TIMESTAMP primary key,
     hour       INT,
     day        INT,
     week       INT,
     month      INT,
     year       INT,
     weekday    INT
  );  """)

songplaytemp_create = ("""CREATE TABLE IF NOT EXISTS songplaytemp
  (
     ts          TIMESTAMP not null,
     userid      INT not null,
     level       TEXT,
     artist      TEXT not null,
     song        TEXT not null,
     sessionid   INT,
     location    TEXT,
     useragent   TEXT
  );  """)

# INSERT RECORDS

songplay_table_insert = ("""  INSERT INTO songplaytemp
            (ts,
             userid,
             level,
             artist,
             song,
             sessionid,
             location,
             useragent)
VALUES      (%s,
             %s,
             %s,
             %s,
             %s,
             %s,
             %s,
             %s)
;

INSERT INTO songplays
            (
             start_time,
             userid,
             level,
             song_id,
             artist_id,
             sessionid,
             location,
             useragent)
SELECT 
       songplaytemp.ts AS start_time,
       songplaytemp.userid,
       songplaytemp.level,
       songs.song_id,
       artists.artist_id,
       songplaytemp.sessionid,
       songplaytemp.location,
       songplaytemp.useragent
FROM   songplaytemp
       LEFT JOIN artists
              ON songplaytemp.artist = artists.artist_name
       LEFT JOIN songs
              ON songplaytemp.song = songs.title
              ;  """)

user_table_insert = (""" INSERT INTO users
            (userid,
             firstname,
             lastname,
             gender,
             level)
VALUES      (%s,
             %s,
             %s,
             %s,
             %s)
ON  CONFLICT (userid) DO UPDATE  
SET    
firstname = excluded.firstname , 
lastname=excluded.lastname,
gender=excluded.gender,
level=excluded.level; """)

song_table_insert = ("""INSERT INTO songs
            (song_id,
             title,
             artist_id,
             YEAR,
             duration)
VALUES      (%s,
             %s,
             %s,
             %s,
             %s)
ON  CONFLICT (song_id) DO UPDATE  
SET   
title=excluded.title,
artist_id=excluded.artist_id,
YEAR=excluded.YEAR,
duration =excluded.duration; """)

artist_table_insert = ("""INSERT INTO artists
            (artist_id,
             artist_name,
             artist_location,
             artist_latitude,
             artist_longitude)
VALUES     (%s,
            %s,
            %s,
            %s,
            %s)
ON  CONFLICT (artist_id) DO UPDATE  
SET 
artist_name=excluded.artist_name,
artist_location=excluded.artist_location,
artist_latitude=excluded.artist_latitude,
artist_longitude=excluded.artist_longitude ; """)

time_table_insert = (""" INSERT INTO time
            (
                        start_time ,
                        hour ,
                        day ,
                        week ,
                        month ,
                        year ,
                        weekday
            )
            VALUES
            (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
            )
ON  CONFLICT (start_time) DO NOTHING; """)



# FIND SONGS

song_select = (""" SELECT songs.song_id,
       artists.artist_id
FROM   songplaytemp
       INNER JOIN artists
               ON songplaytemp.artist = artists.artist_name
       INNER JOIN songs
               ON songplaytemp.song = songs.title; 
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplaytemp_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop,songplaytemp_table_drop]


#songplaytemp.songplay_id ,songplaytemp.ts as start_time ,songplaytemp.userId , songplaytemp.level ,, songplaytemp.sessionId , #songplaytemp.location , songplaytemp.userAgent