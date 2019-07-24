# Project: Data Modeling with Postgres

## Summary

Have you ever wondered how to take raw log files and transform them into a relational database?  With this repository, I will show you how to do it using [pandas](https://pandas.pydata.org/), [Postgres](https://www.postgresql.org/) and  [pscopg2](http://initd.org/psycopg/) in [Python](https://www.python.org/).  You will learn to read log files into a tabular panda's dataframe, use SQL to create a star-scheme perfect for doing aggregations and analytics in python.

## Purpose

Sparkify -- a fictitious startup -- wants to analyze the data they have been collecting on songs and user activity form their new music streaming app. They are particularly interested in finding out what songs are user's are listening to. Their data is stored JSON logs files and needs to be analyzed in order to find out.  They want to create a database optimized to analyze user's listening behavior. To perform this analysis routinely they need a database schema and an extract-transform-and-load (ETL) pipeline.

## Design

How can we find out what songs are subscriber's listening to?  To answer this question I need to restructure the Sparkify log files into a relational database allowing it to be quantified using SQL queries.  Log files of subscriber activities are gathered using Sparkify's online transactional processing (OLTP) system that is optimized for fast writes.  Think log files.  To profit from analysis of user data the larger the data volume the better.  Analyzing this data is the realm of data warehouses that ingest and restructure transactional data for analysis.  Star schemas simplify analytic queries by restructuring the data in a more normalize form.  Think of tables of data where each row has a unique identifier or primary key.  This is know as the second-normal-form and tables of this kind are common in data warehouses.  The idea of star schema is simple, one central fact table that is related to dimension tables by their primary keys.  Star schemas are common in data warehouses -- prevalent example of an online analytical processing systems (OLAP).

## Implementation

PostgreSQL tables are managed using SQL statements that are executed using the Python psycopg2 package.  The star schema is implemented in SQL.  Data files are read using the pandas `read_json` function that returns a dataframe.  Columns and rows from the dataframe are selected and output as tuples for insertion into the database tables.  Connections to the database are managed by psycopg2 as is the cursor object used to interact with the database.  

## Files Descriptions

1. `data` directory - Holds the song data and the log data.

2. `create_tables.py` - Uses `sql_queries.py` to delete and re-create the database and all its tables.  After running this function the database is ready for data to be imported.

3. `environment.yml` - Python packages required to run this application.

4. `etl_prototype.py` - Prototype for the data processing pipeline that loads data from one song and log data file.
 
5. `etl.ipynb` - Exported from `etl.py` using tooling provided by the [Python Plugin](https://code.visualstudio.com/docs/languages/python) for [Visual Studio Code](https://code.visualstudio.com/).

6. `sql_queries.py` - Creates, inserts and drops the tables that implement the star schema.

7. `test.ipynb` - Tests whether data has been inserted into all of the database's tables.

## Running

1. Install: Download this project from Github [https://github.com/robOcity/song_play](https://github.com/robOcity/song_play) by running `git clone https://github.com/robOcity/song_play`.

2. Configure: Configure you Python environment by running `conda env create -f environment.yml`.  Regrettable, if you are using pip you can't there from here.  In other words, conda does not support creating a `requirments.txt` file directly.

3. Run:  
   1. Start and configure your Postgres database (not covered here)
   2. Change directories into the `song_play` directory
   3. Run `python create_tables.py` 
   4. Run `python etl.py`  

## References

1. [Million Song Dataset - FAQ with fields and data types](http://millionsongdataset.com/faq/) - Lists the fields and data-types used in the [Million Song Dataset](http://millionsongdataset.com/).

2. [Converting from Unix Timestamp to PostgreSQL Timestamp or Date](http://www.postgresonline.com/journal/archives/3-Converting-from-Unix-Timestamp-to-PostgreSQL-Timestamp-or-Date.html) - Explans how to go from Unix epoch time to a PostgreSQL timestamp value.

3. [PostgreSQL Keyword List](https://www.postgresql.org/docs/current/sql-keywords-appendix.html) - Note: _USER_ is a reserved keyword in Postgres and cannot be used as a table name.

4. [Psycopg2 - Fast execution helpers](http://initd.org/psycopg/docs/extras.html#fast-execution-helpers) - How to use the `executemany()` method to insert many rows at once into a table.

5. [Using PostgreSQL SERIAL To Create Auto-increment Column](http://www.postgresqltutorial.com/postgresql-serial/) - How to create a primary key that increments automatically.

6. [How to insert current_timestamp into Postgres via python](https://stackoverflow.com/questions/6018214/how-to-insert-current-timestamp-into-postgres-via-python) - Explains how to easily insert timestamps into PostgreSQL by converting them to datetime objects in Python and then letting [pscopg2](http://initd.org/psycopg/) handle the rest. 

7. [Pandas convert dataframe to array of tuples](https://stackoverflow.com/questions/9758450/pandas-convert-dataframe-to-array-of-tuples) - Examples and explanation of how to convert rows of [pandas](https://pandas.pydata.org/) dataframe into tuples for insertion into the database.  

8. [Psycopg2 Extras - Fast execution helpers](http://initd.org/psycopg/docs/extras.html?highlight=executemany) - Explanation and examples of how to insert many records into a table in one transaction using psycopg2's `executemany()` method.  

9. [How to UPSERT (MERGE, INSERT … ON DUPLICATE UPDATE) in PostgreSQL?](https://stackoverflow.com/questions/17267417/how-to-upsert-merge-insert-on-duplicate-update-in-postgresql?noredirect=1&lq=1) - How to handle duplicate primary keys in PostgreSQL INSERT statements that is informally referred to as `upsert`.  
