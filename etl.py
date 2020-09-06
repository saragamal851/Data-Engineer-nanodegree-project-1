import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Reads data from song_data file and save it in dataframe object then
    extract records from df and load it into target tables "songs, artists" in sparkifydb"""
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title','artist_id','year','duration']].drop_duplicates().values
    cur.executemany(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id' , 'artist_name' , 'artist_location' , 'artist_latitude' , 'artist_longitude']].drop_duplicates().values
    cur.executemany(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Reads data from log_data file and save it in dataframe object then
    extract records from df and load it into target tables "time, users" in sparkifydb.This function also inserts records into songplays
    fact table after joining with songs and artists tables"""
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # extract values from timestamp 
    year = (df['ts'].dt.year).tolist()
    day = (df['ts'].dt.day).tolist()
    month = (df['ts'].dt.month).tolist()
    hour = (df['ts'].dt.hour).tolist()
    #start_time= (df['ts']).tolist()
    weekday = (df['ts'].dt.weekday).tolist()
    week= (df['ts'].dt.week).tolist()
    
    # insert time data records
    time_data = (df['ts'], hour, day, week, month, year,weekday)
    column_labels = ( 'start_time','hour', 'day', 'week', 'month', 'year','weekday')
    time_dic=dict(zip(column_labels, time_data))
    time_df =  pd.DataFrame.from_dict(time_dic)
    time_df=time_df.drop_duplicates().values
    cur.executemany(time_table_insert, time_df)

    # load user table
    df.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)
    user_df = df[['userId', 'firstName', 'lastName', 'gender' ,'level' ]]
    user_df=user_df.drop_duplicates().values

    # insert user records
    cur.executemany(user_table_insert, user_df)
    

    # insert songplay records
    songplaytemp_df = df[['ts','userId','level','artist','song','sessionId','location','userAgent']]
    songplaytemp_df=songplaytemp_df.drop_duplicates().values
    cur.executemany(songplay_table_insert, songplaytemp_df)
        

   #for index, row in df.iterrows():
#       
#       # get songid and artistid from song and artist tables
#       cur.executemany(song_select, (row.song, row.artist, row.length))
#       results = cur.fetchone()
#       
#       if results:
#           songid, artistid = results
#       else:
#           songid, artistid = None, None
#
#       # insert songplay record
#       songplay_data = 
#       cur.executemany(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """loads all json datasets from dictionary"""
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
    """Connects to the Sparkify database, extracts and processes the log_data and song_data, and loads data into the five tables by
    calling process_data function that calls process_song_file and process_log_file functions """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()