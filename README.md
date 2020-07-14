## Project: Data Warehouse

### Summary

This project builds an ETL pipeline for Sparkify, a music streaming startup, that wants to move their processes and data to the cloud. The data is extracted from Amazon S3 and stages it in a database hosted on Redshift and loads data into a set of fact and dimensional tables. The goal is to help the analytics team identify the songs users are listening to.

### Database Schema

A star schema was used to design the database to optimize queries on song play analysis. An ETL pipeline using Python and SQL queries extracts the data from the song and log datasets to a  database hosted on Redshift. Below are the tables included in the database schema:

**Fact Table**

**songplays** - records in log data associated with song plays i.e. records with page `NextSong`
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**

**users** - users in the app
- user_id, first_name, last_name, gender, level

**songs** - songs in music database
- song_id, title, artist_id, year, duration

**artists** - artists in music database
- artist_id, name, location, latitude, longitude

**time** - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

### Instructions

Update the `dwh.cfg` config file with Amazon Redshift CLUSTER and IAM_ROLE credentials.

From the terminal, run `python create_tables.py` to create tables and `python etl.py` to start ETL pipeline.
