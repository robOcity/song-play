# Project: Data Modeling with Postgres

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a [Postgres](https://www.postgresql.org/) database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description

In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## References

1. [Million Song Dataset - FAQ with fields and data types](http://millionsongdataset.com/faq/)
2. [Converting from Unix Timestamp to PostgreSQL Timestamp or Date](http://www.postgresonline.com/journal/archives/3. Converting-from-Unix-Timestamp-to-PostgreSQL-Timestamp-or-Date.html)
4. [PostgreSQL Keyword List](https://www.postgresql.org/docs/current/sql-keywords-appendix.html) - Note: _USER_ is a reserved keyword in Postgres and cannot be used as a table name.  
5. [Passing python variable to sql statement psycopg2 pandas](https://stackoverflow.com/questions/38317601/passing-python-variable-to-sql-statement-psycopg2-pandas)
6. [Auto incrementing primary key in postgresql](https://stackoverflow.com/questions/7718585/how-to-set-auto-increment-primary-key-in-postgresql)
