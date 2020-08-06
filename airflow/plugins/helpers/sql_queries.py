class SqlQueries:
    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{{}}'
        SECRET_ACCESS_KEY '{{}}'
        REGION '{}'
        FORMAT AS JSON '{}';
"""
    COPY_EVENTS_SQL = COPY_SQL.format(
    "staging_events",
    "s3://udacity-dend/log_data",
    "us-west-2",
    "s3://udacity-dend/log_json_path.json"
)
    COPY_SONGS_SQL = COPY_SQL.format(
    "staging_songs",
    "s3://udacity-dend/song_data",
    "us-west-2",
    "auto"
)
    TRUNCATE_TABLE = """
        TRUNCATE TABLE {}
"""
    
    songplay_table_insert = ("""
        INSERT INTO songplays(songplay_id, start_time,user_id,level,song_id,artist_id, duration,sessionid,location,userAgent)
        SELECT
        		md5(events.sessionid || events.start_time) songplay_id,
                CAST(events.start_time AS timestamp), 
                CAST(events.userid as int), 
                events.level, 
                songs.song_id, 
                songs.artist_id,
                CAST(songs.duration as decimal(10,4)),
                CAST(events.sessionid as int),
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            WHERE songs.song_id is not null
                AND songs.artist_id is not null;
    """)

    user_table_insert = ("""
        INSERT INTO users(user_id, firstname, lastname, gender, level)
        SELECT distinct CAST(userid as int), firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO songs(song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists(artist_id, artistname, artist_location, artist_latitude, artist_longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
 		INSERT INTO time(start_time, hour, day, week, month, year, weekday)   
        SELECT start_time, extract(hour from start_time) as hour, extract(day from start_time) as day, extract(week from start_time) as week, 
               extract(month from start_time) as month, extract(year from start_time) as year, extract(dayofweek from start_time) weekday
        FROM songplays
    """)
    
    tables = ["songplays","users","songs","artists","time"]
    
    tests = [{"test":" SELECT COUNT(*) from songplays" , "expected_result":"119"} , 
        {"test":" SELECT COUNT(*) from users" , "expected_result":"104"},
        {"test":" SELECT COUNT(*) from songs" , "expected_result":"14896"},
        {"test":" SELECT COUNT(*) from artists" , "expected_result":"10025"},
        {"test":" SELECT COUNT(*) from time" , "expected_result":"119"},
        ]
    
    