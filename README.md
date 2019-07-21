# Project: Data Modeling with Postgres

## Summary

Have you ever wondered how to take raw log files and transform them into a relational database?  In this repository, I will show you how using [pandas](https://pandas.pydata.org/), [Postgres](https://www.postgresql.org/) and [Python](https://www.python.org/).  You will see how to read log files into a tabular panda's dataframe, use SQL to create a star-scheme to do aggregations and analytics and glued together with python.   

## Purpose

Sparkify -- a fictitious startup -- wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They are particularly interested in understanding what songs user's are listening to. Currently, their data, stored JSON logs files, is difficult to analyze.  They want to create a database optimized to analyze which songs user's are playing. To perform this analysis they envision a database schema and extract-transform-load (ETL) pipeline.

## Analytic Needs

What songs are subscriber's listening to?  To answer this question I am going to restructure the Sparkify log files into a relational database for its analytical capabilities.  Log files of subscriber activities are gathered using Sparkify's Online Transactional Processing System that is optimized for fast writes.  Think log files.  Profiting from analysis of user data improves with increased volume, greater integrity and minimal redundancy.  This is the realm of data warehouses, and ingesting transactional data requires the data that it be restructured.  Star schemas help to normalize the data so that desired queries are simplified.  Think of tables of data where each row has a unique identifier or primary key.  This is know as the second-normal-form and tables of this kind are common in data warehouses.  The idea of star schema is simple, one central fact table that is related to dimension tables by their primary keys.  

## Implementation



## Files Descriptions
1. `data` directory - Holds the song data and the log data. 
 
2. `create_tables.py` - Uses `sql_queries.py` to first delete and re-create the database and necessary tables.  Note: Data is not imported.

3. `environment.yml` - Python packages required to run this application. 

4. `etl_prototype.py` - Prototype for the data processing pipeline. 
 
5. `etl.ipynb` - Exported from `etl.py` using tooling provided by the [Python Plugin](https://code.visualstudio.com/docs/languages/python) for [Visual Studio Code](https://code.visualstudio.com/).

6. `sql_queries.py` - Creates, inserts and drops the tables that implement the the star schema.

7. `test.ipynb` - Tests whether data has been inserted into all of the database's tables.

## Running

1. Install: Download this project from Github [https://github.com/robOcity/song_play](https://github.com/robOcity/song_play) by running `git clone https://github.com/robOcity/song_play`. 
 
2. Configure: Configure you Python environment by running `conda env create -f environment.yml`.  Regrettable, if you are using pip you can't there from here.  In other words, conda does not support creating a `requirments.txt` file directly. 
 
3. Run:  In your terminal, change directories into the `song_play`  directory.  Then run `python etl.py`.  

## References

1. [Million Song Dataset - FAQ with fields and data types](http://millionsongdataset.com/faq/) - Lists the fields and datatypes used in the [Million Song Dataset](http://millionsongdataset.com/).
   
2. [Converting from Unix Timestamp to PostgreSQL Timestamp or Date](http://www.postgresonline.com/journal/archives/3. Converting-from-Unix-Timestamp-to-PostgreSQL-Timestamp-or-Date.html)

3. [PostgreSQL Keyword List](https://www.postgresql.org/docs/current/sql-keywords-appendix.html) - Note: _USER_ is a reserved keyword in Postgres and cannot be used as a table name. 
 
4. [Psycopg2 - Fast execution helpers](http://initd.org/psycopg/docs/extras.html#fast-execution-helpers) - How to use the `executemany()` method to insert many rows at once into a table.

5. [Using PostgreSQL SERIAL To Create Auto-increment Column](http://www.postgresqltutorial.com/postgresql-serial/) - How to create a primary key that increments automatically.

6. [How to insert current_timestamp into Postgres via python](https://stackoverflow.com/questions/6018214/how-to-insert-current-timestamp-into-postgres-via-python) - Explains how to easily insert timestamps into PostgreSQL by converting them to datetime objects in Python and then letting [pscopg2](http://initd.org/psycopg/) handle the rest. 

7. [Pandas convert dataframe to array of tuples](https://stackoverflow.com/questions/9758450/pandas-convert-dataframe-to-array-of-tuples) - Examples and explanation of how to convert rows of [pandas](https://pandas.pydata.org/) dataframe into tuples for insertion into the database.  

8. [Psycopg2 Extras - Fast execution helpers](http://initd.org/psycopg/docs/extras.html?highlight=executemany) - Explanation and examples of how to insert many records into a table in one transaction using psycopg2's `executemany()` method.  

9.  [How to UPSERT (MERGE, INSERT … ON DUPLICATE UPDATE) in PostgreSQL?](https://stackoverflow.com/questions/17267417/how-to-upsert-merge-insert-on-duplicate-update-in-postgresql?noredirect=1&lq=1) - How to handle duplicate primary keys in PostgreSQL INSERT statements that is informally referred to as `upsert`.  
