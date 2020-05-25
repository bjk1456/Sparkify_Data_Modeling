# DROP TABLES

songplay_table_drop = "DROP TABLE if exists songplays CASCADE;"
user_table_drop = "DROP TABLE if exists users CASCADE;"
song_table_drop = "DROP TABLE if exists songs CASCADE;"
artist_table_drop = "DROP TABLE if exists artist CASCADE;"
time_table_drop = "DROP TABLE if exists time CASCADE;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays ( songplay_id serial primary key, song_id varchar(255), artist_id varchar(255), ts timestamp(3) with time zone, level char(10), sessionId integer, location varchar(255), userAgent text);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users ( userId varchar(255) primary key, firstName varchar(255), lastName varchar(255), gender char, level varchar(255));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs ( song_id varchar(255) primary key, title varchar(255) NOT NULL, artist_id varchar(255), year integer, duration numeric(10,5));
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists ( artist_id varchar(255) primary key, artist_name varchar(255) NOT NULL, artist_location varchar(255), artist_point point);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (  time_id serial primary key, hour smallint, day smallint, week_of_year smallint, month smallint, year integer, title varchar(255));
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays(song_id, artist_id, ts, level, sessionId, location, userAgent)
VALUES(%s,%s,to_timestamp(%s),%s,%s,%s,%s)
""")

user_table_insert = ("""
  INSERT INTO users(userId, firstName, lastName, gender, level)
       VALUES (%s,%s,%s,%s,%s)
  ON CONFLICT (userID)
DO UPDATE SET level=EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
     VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (song_id)
DO NOTHING;
""")

artist_table_insert = ("""
  INSERT INTO artists(artist_id, artist_name, artist_location, artist_point)
       VALUES (%s,%s,%s,point(%s,%s))
  ON CONFLICT (artist_id)
DO UPDATE SET artist_name=EXCLUDED.artist_name, artist_location=EXCLUDED.artist_location, artist_point=EXCLUDED.artist_point;
""")


time_table_insert = ("""
INSERT INTO time(hour, day, week_of_year, month, year, title)
     VALUES (%s,%s,%s,%s,%s,%s)
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, a.artist_id 
      FROM songs s
INNER JOIN artists a ON s.artist_id = a.artist_id 
     WHERE s.title=%s AND a.artist_name=%s AND s.duration=%s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]