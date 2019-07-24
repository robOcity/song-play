# %% [markdown]
# # ETL Prototype
# Establish data processing workflow for a small subset of the data.

# %%
import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path
from sql_queries import *

# %% [markdown]
# ## Connect to Postgres Database
# After connecting to the database and getting a cursor object, then drop and recreate all tables.

# %%
conn = psycopg2.connect(
    "host=127.0.0.1 dbname=sparkifydb user=student password=student"
)
conn.set_session(autocommit=True)
cur = conn.cursor()
for sql_cmd in drop_table_queries + create_table_queries:
    cur.execute(sql_cmd)

# %% [markdown]
# ## Find data files for processing
#
# Use `os.walk` to find all `*.json` files under the `filepath` directory.

# %%
# Let's apply the DRY principle and write a function to load our
# data.


def get_files(filepath):
    """Return all JSON files under filepath as a list"""
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


# %% [markdown]
# ## #1: `song` Table
# #### Extract Data for Song Table
#
# Process `song_data` by reading in a subset of the [Million Song Dataset](http://millionsongdataset.com/) and in the process extracting data from JSON files using pandas.

# %%
song_root_dir = Path().cwd() / "data" / "song_data"
song_files = get_files(song_root_dir)
filepath = song_files[0]
df = pd.read_json(filepath, lines=True)

# %% [markdown]
# #### Insert Data into the Song Table
#
# - Method 1: select columns and return as a tuple knowing that there is one song per dataframe and results in __year as typye np.int64 and duration as type np.float64__.  Pandas uses numpy to store its numeric types, so this result is expected.

# %%
song_data = next(
    df[["song_id", "title", "artist_id", "year", "duration"]].itertuples(
        index=False, name=None
    )
)

# %% [markdown]
# - Method 2: Select columns, select first row, get values as numpy array and convert to a list that results in __year as typye int and duration as type float__.  Inserting numpy numeric types into the database using psycopg2 causes errors, so I will use this approach.  This type conversion occurs because it is behavior of [numpy.ndarray.tolist](https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.tolist.html#numpy.ndarray.tolist) upon which [pandas.Series.tolist](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.tolist.html) is based.  Mystery solved!

# %%
# Select and insert data into the songs table
song_df = df[["song_id", "title", "artist_id",
              "year", "duration"]]
song_df.head()

# %%
song_data = song_df.values[0].tolist()
song_data = [x if x else None for x in song_data]
cur.execute(song_table_insert, song_data)

# %% [markdown]
# ## #2: `artists` Table
# #### Extract Data for Artist Table
#
# Extract data and insert into artist table.

# %%
artist_df = (
    df[
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ]
)
artist_df.head()

# %%
artist_data = artist_df.values[0].tolist()
cur.execute(artist_table_insert, artist_data)

# %% [markdown]
# # Process `log_data`
#
# Now let's add the subscriber activity data to see which songs are popular.

# %%
log_data_root = Path().cwd() / "data" / "log_data"
log_files = get_files(log_data_root)
# just read first file to test functionality
filepath = log_files[0]
df = pd.read_json(filepath, lines=True)

# %% [markdown]
# ## #3: `time` Table
# #### Extract and Insert Data into Time Table
#
# Find what songs user's are choosing by just considering `NextSong` records.  Then convert the `ts` timestamp column to datetime and extract columns for hour, day, week of year, month, year, and weekday (see: [Accessors](https://pandas.pydata.org/pandas-docs/stable/reference/series.html#time-series-related) [dt Accessor](https://pandas.pydata.org/pandas-docs/stable/reference/series.html#api-series-dt) that allows datetime properties to be easily accessed).

# %%
df = df.assign(ts=pd.to_datetime(df.ts, unit="ms"))
df = df.loc[df.page.isin(["NextSong"])]
df = df.assign(timestamp=pd.to_datetime(df.ts, unit="ms"))
df.timestamp = df.timestamp.dt.tz_localize("UTC")

# %%
time_df = pd.DataFrame(
    {
        "timestamp": df.timestamp,
        "hour": df.timestamp.dt.hour,
        "day": df.timestamp.dt.day,
        "week_of_year": df.timestamp.dt.week,
        "month": df.timestamp.dt.month,
        "year": df.timestamp.dt.year,
        "weekday": df.timestamp.dt.weekday,
    }
)
# Here we want native pandas datatypes, so I'll user iterrows.
for i, row in time_df.iterrows():
    cur.execute(time_table_insert, list(row))

# %%
time_df.head()

# %% [markdown]
# ## #4: `users` Table
# #### Extract and Insert Data into Users Table
#
# Every time a user plays a song they appear in the log file, so naturally there will by duplicate userId entries.  Here we remove them to create a normalized user table.
# %%
user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
user_df = user_df.drop_duplicates(subset="userId", keep="last")
user_df.head()

# %%
for i, row in user_df.iterrows():
    cur.execute(user_table_insert, row)

# %% [markdown]
# ## #5: `songplays` Table
# #### Extract and Insert Data and Songplays Table
#
# To look up song or an artist, I need the unique identifier or primary key. The log files simply have the name of the song and artist.  So, I need to do a reverse lookup up to get identifiers.
#
# ```sql
# SELECT s.song_id, a.artist_id FROM dim_song s
# JOIN dim_artist a ON s.artist_id = a.artist_id
# WHERE s.title = %s AND a.name = %s AND s.duration = %s;
# ```
#
# Iterating over the rows of the dataframe holding the log data.  First, I extract the find the unique identifiers, Next, I combine them with other data from the log data to insert the user's songplay activity into the `song_play` table.

# %%
for index, row in df.iterrows():

    # get songid and artistid from song and artist tables
    cur.execute(song_select, (row.song, row.artist, row.length))
    results = cur.fetchone()

    if results:
        songid, artistid = results
    else:
        songid, artistid = None, None

    # insert songplay record
    songplay_data = (
        row.userId,
        songid,
        artistid,
        row.sessionId,
        row.ts,
        row.level,
        row.location,
        row.userAgent,
    )
    cur.execute(songplay_table_insert, songplay_data)

# %% [markdown]
# ## Close Connection to Sparkify Database

# %%
conn.close()
