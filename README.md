# Project: Data Modeling with Postgres

## Purpose
This project aims to provide a database so that Sparkify's analytics area can analyze and understand the behavior of its users.

## Project Overview
A ETL pipeline was created for them, that will extract data from JSON files, transform and load the data into their PostgreSQL database following a star schema model.

### Star schema
The DB consists of the following tables organized in a star schema:
-   Fact table: songplays
-   Dimension table: users, songs, artists and time

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
