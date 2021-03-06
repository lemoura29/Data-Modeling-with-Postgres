# Project: Data Modeling with Postgres

## Purpose
This project aims to provide a database so that Sparkify's analytics area can analyze and understand the behavior of its users.

## Project Overview
A ETL pipeline was created for them, that will extract data from JSON files, transform and load the data into their PostgreSQL database following a star schema model.

### Star schema
The DB consists of the following tables organized in a star schema:
-   Fact table: songplays
-   Dimension table: users, songs, artists and time

![alt text](https://github.com/lemoura29/Data-Modeling-with-Postgres/blob/main/modelagem.png)

### Datastes
#### Song Dataset
This dataset contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

####  Log Dataset
This dataset contains the JSON files for user songplay log data, they are partitioned by year and month.

### Project Files

**sql_queries.py** : This file contains SQL commands used to DROP, CREATE, INSERT and SELECT data tables.

**create_tables.py**  - When executed it drops the tables and creates the tables as defined in the sql_queries file.

**etl.py**  - This script does the extraction, transforming and loading the data into the tables.

To assist we have:

**etl.ipynb** - This Jupyter Notebook file contains steps of the ETL Process 

**test.ipynb** - This Jupyter Notebook file contains SQL commands to test if was successfully loaded the data into the database.

## How to run the project

1.  Execute the "python create_tables.py" file in the Terminal to create all tables.
2.  Execute the "python etl.py" file in the Terminal to insert all records in the tables.

## Example Queries

#### Amount of music heard per day of the week
~~~ 
SELECT case
           when weekday = 0 then 'Sunday'
           when weekday = 1 then 'Monday'
           when weekday = 2 then 'Tuesday'
           when weekday = 3 then 'Wednesday'
           when weekday = 4 then 'Thursday'
           when weekday = 5 then 'Friday'
           when weekday = 6 then 'Saturday' end as weekday,
       COUNT(songplay_id)                          songs
FROM songplays s
         JOIN time t ON s.start_time = t.start_time
GROUP BY 1
ORDER BY songs DESC 
~~~

#### Amount of music heard by gender

```
SELECT case when gender = 'F' then 'Woman' else 'Man' end as gender, COUNT(*)
FROM songplays s
         JOIN users u ON s.user_id = u.user_id
GROUP BY 1
ORDER BY gender DESC
```

####  Duration by artist
```
SELECT a.name, SUM(duration) 
                      FROM songplays s 
                      JOIN artists a ON s.artist_id = a.artist_id 
                      JOIN songs sg ON sg.song_id = s.song_id 
                      GROUP BY 1
```

#### Amount by level
```
SELECT level, COUNT(*) status 
                 FROM songplays 
                 GROUP BY 1 
                 ORDER BY status DESC
```
