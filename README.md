Project: Data Modeling with Postgres

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.
Project Description

In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.


Schema for Song Play Analysis

Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

Fact Table
    songplays - records in log data associated with song plays i.e. records with page NextSong
      “  songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent”
      
Dimension Tables
    users - users in the app
        user_id, first_name, last_name, gender, level
    songs - songs in music database
        song_id, title, artist_id, year, duration
    artists - artists in music database
        artist_id, name, location, latitude, longitude
    time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday



* as shown in uploaded image named model.png the fact table is songplay table related with four dimension table (song, artist, user, time).
* Artist table contains general data about artists like artist name and gender.
* Song table contains all songs in the library with data that describes artist, song duration and release date.
* time table is the date and time dimension for the start schema model.
* users table contains all registered users that using the music library.

to load these dimension tables, primary key should be identified for each dimension. according that small analysis happened using sql queries to identify the uniqness for each column.

----------------------------------
during data loding some duplication existed, for dimension data will be updated except time table because time table calculated during runtime.

loading steps:

1- Execute create_tables.py to create the Tables or replace them if existed.

2- Execute etl.py to Load the into postgres database.

implemented ETL do below steps:

1- Reading the Jason Log Files and songs files that existed in Directory, Count them, and prints to the console how many files found for processing.

2- form each jason file identify the required column that we need and loop between all files, then finally load required data to database.

3- if the record existed before in dimension table will be updated except time dimension.