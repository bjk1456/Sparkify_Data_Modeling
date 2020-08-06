class CreateTableQueries:
    # DROP TABLES

    staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
    staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
    songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
    user_table_drop = "DROP TABLE IF EXISTS users;"
    song_table_drop = "DROP TABLE IF EXISTS songs;"
    artist_table_drop = "DROP TABLE IF EXISTS artists;"
    time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
    create_stagging_events_table = """
        CREATE TABLE IF NOT EXISTS staging_events (
            artist varchar(255),
            auth varchar(25),
            firstName varchar(255),
            gender varchar(255),
            itemInSession varchar(255),
            lastName varchar(255),
            length varchar(255),
            level varchar(25),
            location varchar(255),
            method varchar(25),
            page varchar(255),
            registration varchar(255),
            sessionid varchar(255),
            song varchar(255),
            status varchar(255),
            ts varchar(255),
            userAgent varchar(510),
            userid varchar(255)
            );
    """
    
    create_stagging_songs_table = """
        CREATE TABLE IF NOT EXISTS staging_songs (
            song_id varchar(255),
            num_songs integer,
            title varchar(255),
            artist_name varchar(255),
            artist_latitude float,
            artist_longitude float,
            year int,
            duration float,
            artist_id varchar(255),
            artist_location varchar(255)
    );
    """
    
    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id varchar(510) PRIMARY KEY,
            start_time timestamp not null,
            user_id integer not null,
            level varchar(25),
            song_id varchar(255) not null,
            artist_id varchar(255) not null,
            duration decimal(10,4),
            sessionid int,
            location varchar(255),
            userAgent varchar(255),
            FOREIGN KEY(user_id) REFERENCES users(user_id),
            FOREIGN KEY(song_id) REFERENCES songs(song_id),
            FOREIGN KEY(artist_id) REFERENCES artists(artist_id),
            FOREIGN KEY(start_time) REFERENCES time(start_time)
    );
    """)
    
    """users in the app"""
    user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer PRIMARY KEY,
        firstname varchar(255),
        lastname varchar(255),
        gender char,
        level varchar(25)
    );
    """)

    """songs in music database"""
    song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar(255) PRIMARY KEY,
        title varchar(255),
        year int,
        duration decimal(10,4),
        artist_id varchar(255) not null,
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    );
    """)

    """artists in music database"""
    artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar(255) PRIMARY KEY,
        artistname varchar(255),
        artist_location varchar(255),
        artist_latitude numeric(8,4),
        artist_longitude numeric(8,4)
    );
    """)

    """timestamps of records in songplays broken down into specific units"""
    time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY,
        hour smallint, 
        day smallint,
        week smallint,
        month smallint,
        year int,
        weekday smallint
    );
    """)
    
    

    drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
    create_table_queries = [create_stagging_events_table, create_stagging_songs_table, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
    