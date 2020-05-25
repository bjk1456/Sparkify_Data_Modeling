# Sparkify data analysis

Sparkify, a new startup, has a music streaming app that has been running - collecting various data. The analytics team wanted to find out what songs its users were listening to. In order to meet the analytics team's objectives I created a database schema and ETL pipeline. 

The data being collected consists of 2 different JSON files: log and song.

Log data consists of the following fields: artist, auth, firstName, gender, itemInSession, lastName, length, level, location, and userId.

Song data consists of the following fields: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, and year.

The 5 tables that I created from the above fields can be found in the sql_queries.py file. 

I went with a Star Schema to answer the analytics team's objectives. The 4 facts tables are as follwing: time, artist, song, user.

Fields extracted from the song files were loaded into the song and artists tables. The user and time tables were fed from the log files. The fact table songplays is at the center of the star schema as a fact table. Using the songplays table we can answer questions such as "What's the most/least popular song on Sparkify?" and "Who's the most/least popular artist?"