# Data modeling Postgres assignment

## Project Summary
Using the song and log datasets this project creates a star schema optimized for queries on song play analysis. 
This includes the following tables.

### Schema for Song Play Analysis

Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.
#### Fact Table

* songplays - records in log data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

* users - users in the app
        user_id, first_name, last_name, gender, level
* songs - songs in music database
        song_id, title, artist_id, year, duration
* artists - artists in music database
        artist_id, name, location, latitude, longitude
* time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday


## Relevant files

* Data
    * song_data: JSON files that contains information about songs and artists
    * log_data:  JSON files that contains event logs information.

* Scripts
    * create_tables.py drops and creates your tables. Run this file to reset your tables BEFORE EACH TIME you run your ETL scripts.
    * sql_queries.py: contains all SQL sentences: creation, drop and queries.
    * etl.py: reads and processes files from song_data and log_data and loads them into the tables.
    * etl.ipynb reads and processes a single file from song_data and log_data and loads the data into your tables. 
      This notebook contains detailed instructions on the ETL process for each of the tables.

## How to run scripts

Open a terminal and execute first:

```python
python create_tables.py
python etl.py
```

## Results

### Database query
```sql
SELECT * FROM songplays sp 
JOIN artists a ON a.artist_id = sp.artist_id 
JOIN songs s ON s.song_id = sp.song_id 
JOIN "time" t ON t.start_time = sp.start_time 
```

### Expected result after executing etl.py

Name     |Value             |
---------|------------------|
song_id  |SOZCTXZ12AB0182364|
title    |Setanta matins    |
artist_id|AR5KOSW1187FB35FF4|
year     |0                 |
duration |269.58322         |


songplay_id|start_time         |user_id|level|song_id           |artist_id         |session_id|location                          |user_agent                                                                                                                               |artist_id         |name |location |latitude|longitude|song_id           |title         |artist_id         |year|duration |start_time         |hour|day|week|month|year|weekday|
-----------|-------------------|-------|-----|------------------|------------------|----------|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|------------------|-----|---------|--------|---------|------------------|--------------|------------------|----|---------|-------------------|----|---|----|-----|----|-------|
       4627|2018-11-21 21:56:47|     15|paid |SOZCTXZ12AB0182364|AR5KOSW1187FB35FF4|       818|Chicago-Naperville-Elgin, IL-IN-WI|"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"|AR5KOSW1187FB35FF4|Elena|Dubai UAE|49.80388| 15.47491|SOZCTXZ12AB0182364|Setanta matins|AR5KOSW1187FB35FF4|   0|269.58322|2018-11-21 21:56:47|  21| 21|  47|   11|2018|      2|



