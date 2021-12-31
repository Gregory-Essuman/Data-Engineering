import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Function Description:
    ====================
    This function(process_song_file) performs ETL on song_data to create the songs and artists dimensional tables. 
    
    Function Parameters:
    ====================
    cur      ==>     Database cursor
    filepath ==>     Path to song_data JSON files

    Return Value:  
    =============
    None
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Function Description:
    ====================
    This function(process_log_file) performs ETL on log_data to create the time and users dimensional tables, along with songplays fact table.
    
    Function Parameters:
    ====================
    cur       ==>   Database cursor
    filepath  ==>   Path to log_data JSON file

    Return Value:    
    =============
    None
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True) 

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'] 

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms') 
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for idx, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for idx, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for idx, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Function Description:
    =====================
    This function(process_data) processes all files within the filepath directory through the input function.
    
    Function Parameters:
    ====================
    cur         ==>      Database cursor
    conn        ==>      Connection to Database
    filepath    ==>      Path to dataset file
    func        ==>      Process function

    Return Value:
    =============
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


def main():
    """
    Function Description:
    =====================
    This function(main) builds the ETL Pipeline for Sparkify song play data.

    Function Parameters:
    ====================
    None

    Return Value:
    ============-
    None  
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=Characters1")
    cur  = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data',  func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()