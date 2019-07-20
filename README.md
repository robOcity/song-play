# Project: Data Modeling with Postgres

## Summary

Have you ever wondered how to take raw log files and transform them into a relational database?  In this repository, I will show you how using [pandas](https://pandas.pydata.org/), [Postgres](https://www.postgresql.org/) and [Python](https://www.python.org/).  You will see how to read log files into a tabular panda's dataframe, use SQL to create a star-scheme to do aggregations and analytics and glued together with python.   

## Scenario
Sparkify -- a fictitious startup -- wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They are particularly interested in understanding what songs user's are listening to. Currently, their data, stored JSON logs files, is difficult to analyze.  They want to create a database optimized to analyze which songs user's are playing. To perform this analysis they envision a database schema and extract-transform-load (ETL) pipeline.

## Analytic Needs

What songs are subscriber's listening to?  To answer this question I am going to restructure the Sparkify log files into a relational database for its analytical capabilities.  Log files of subscriber activities are gathered using Sparkify's Online Transactional Processing System that is optimized for fast writes.  Think log files.  Profiting from analysis of user data improves with increased volume, greater integrity and minimal redundancy.  This is the realm of data warehouses, and ingesting transactional data requires the data that it be restructured.  Star schemas help to normalize the data so that desired queries are simplified.  Think of tables of data where each row has a unique identifier or primary key.  This is know as the second-normal-form and tables of this kind are common in data warehouses.  The idea of star schema is simple, one central fact table that is related to dimension tables by their primary keys.  



## Implementation



## Files

## References

1. [Million Song Dataset - FAQ with fields and data types](http://millionsongdataset.com/faq/)
2. [Converting from Unix Timestamp to PostgreSQL Timestamp or Date](http://www.postgresonline.com/journal/archives/3. Converting-from-Unix-Timestamp-to-PostgreSQL-Timestamp-or-Date.html)
3. [PostgreSQL Keyword List](https://www.postgresql.org/docs/current/sql-keywords-appendix.html) - Note: _USER_ is a reserved keyword in Postgres and cannot be used as a table name.  
4. [Passing python variable to sql statement psycopg2 pandas](https://stackoverflow.com/questions/38317601/passing-python-variable-to-sql-statement-psycopg2-pandas)
5. [Auto incrementing primary key in postgresql](https://stackoverflow.com/questions/7718585/how-to-set-auto-increment-primary-key-in-postgresql)
6. [How to insert current_timestamp into Postgres via python](https://stackoverflow.com/questions/6018214/how-to-insert-current-timestamp-into-postgres-via-python)
7. [Pandas convert dataframe to array of tuples](https://stackoverflow.com/questions/9758450/pandas-convert-dataframe-to-array-of-tuples)
8. [Psycopg2 Extras - Fast execution helpers¶](http://initd.org/psycopg/docs/extras.html?highlight=executemany)
9.  [How to UPSERT (MERGE, INSERT … ON DUPLICATE UPDATE) in PostgreSQL?](https://stackoverflow.com/questions/17267417/how-to-upsert-merge-insert-on-duplicate-update-in-postgresql?noredirect=1&lq=1)
