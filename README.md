# Data Lake Sparkify

## Context

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data is stored in JSON logs on user activity on the app and in JSON metadata on the songs in their app.

In order to build their datalake Spark is going to be used to build an ETL pipleline to build dimensional tables that can be later on used for analysis. The idea is to used this tables for insights in what songs their users are listening to.



## Objective

* Use Spark locally to build an ETL that converts raw data of logs and song metadata into five dimensional tables.

  **Fact Table:**
  + `songplays`: records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

  **Dimensional Tables:**
  + `users`: users in the app
    - user_id, first_name, last_name, gender, level
  + `songs` - songs in music database
    - song_id, title, artist_id, year, duration
  + `artists` - artists in music database
    - artist_id, name, location, lattitude, longitude
  + `time` - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

## Directory structure:

* `data`: Sample raw data of logs and song metadata is stored.
* `etl.py`: pyspark code to transform data into dimensional tables stored in parquet files.
* `dl.cfg`: configuration file with AWS keys.

## Instructions:

### Run Locally:

* Unzip the data: `unzip log-data.zip` and `song-data.zip`
* Open the terminal and execute the etl: `python etl.py`

