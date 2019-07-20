# Project: Data Modeling with Postgres

## Summary

Have you ever wondered how to take raw log files and transform them into a relational database?  In this repository, I will show you how using [pandas](https://pandas.pydata.org/), [Postgres](https://www.postgresql.org/) and [Python](https://www.python.org/).  You will see how to read log files into a tabular panda's dataframe, use SQL to create a star-scheme to do aggregations and analytics and glued together with python.   

## Scenario
Here is the scenario posed for this project:

> A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
>
> They'd like a data engineer to create a PostgreSQL database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Analytic Needs

What songs are subscriber's listening to?  To answer this question I am going to restructure the Sparkify log files into a relational database for its analytical capabilities.  Log files of subscriber activities are gathered using Sparkify's Online Transactional Processing System optimized for fast writes.  To perform the analysis, I will use a star-schema to organize the database's tables.  

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
