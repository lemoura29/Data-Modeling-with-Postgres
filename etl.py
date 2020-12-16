import os
import glob
import psycopg2
import pandas as pd
from io import StringIO
from sql_queries import *


def process_song_file(cur, filepath):
    """
        Description: This function can be used to read the file in the filepath (data/log_data)
        to get the user and time info and used to populate the users and time dim tables.

        Arguments:
            cur: the cursor object. 
            filepath: log data file path. 

        Returns:
            None
    """
    
    # open song file
    df =  pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id","title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id","artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        Description: Read log_file and insert the data into tables: time, users and songplay

        Arguments:
            cur: the cursor object. 
            filepath: log data file path. 

        Returns:
            None
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[(df.page == 'NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts,unit='ms')
    
    # insert time data records
    time_data = (t.values,t.dt.hour.values,t.dt.day.values,t.dt.week.values,t.dt.month.values,t.dt.year.values,t.dt.weekday.values)
    column_labels =  ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId","firstName","lastName", "gender", "level"]]
    user_df = user_df.dropna(subset=['userId'])


    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'),row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        Description: Processes JSON files for a data directory path according to the function passed by parameter. 

        Arguments:
            cur: the cursor object. 
            filepath: log data file path. 
            conn: database connection
            func: function will be executed

        Returns:
            None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
        
def execute_query(cur, func, text):
    """
        Description: executes and prints the query results passed by parameter. 

        Arguments:
            cur: the cursor object. 
            func: query will be executed.
            text: query description

        Returns:
            None
    """
    print(text)
    cur.execute(func)
    results = cur.fetchall()
    
    for row in results:
        print(f'{row[0]} - {row[1]}')

def execute_query_exemples(cur):
    """
        Description: calls function 'execute_query' passing each example query. 

        Arguments:
            cur: the cursor object. 
            func: query will be executed.
            text: query description

        Returns:
            None
    """
    
    print('\n###  QUERY EXAMPLES  ###')
    execute_query(cur, COUNT_WEEKDAY, '\n1- Amount of music heard per day of the week')
    execute_query(cur, AMOUNT_SONG_GENDER, '\n2- Amount of music heard by gender' )
    execute_query(cur, DURATION_ARTIST,'\n3- Duration by artist')
    execute_query(cur, LEVEL_SONG, '\n4- Amount by level')


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    execute_query_exemples(cur)

    conn.close()


if __name__ == "__main__":
    main()