# Schema for Song Play Analysis

A Star Schema would be required for optimized queries on song play queries

## Fact Table

- **songplays** - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables

- **users** - users in the app
user_id, first_name, last_name, gender, level

- **songs** - songs in music database
song_id, title, artist_id, year, duration

- **artists** - artists in music database
artist_id, name, location, lattitude, longitude

- **time** - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

- **Project Template**

# Project Template include four files:

**1. create_table.py** is where to create your fact and dimension tables for the star schema in Redshift.

**2. etl.py** is where to load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

**3. sql_queries.py** is where to define you SQL statements, which will be imported into the two other files above.

**4. README.md**</b>** is where to provide discussion on your process and decisions for this ETL pipeline.

# Create Table Schema

1. Write a SQL CREATE statement for each of these tables in sql_queries.py
2. Complete the logic in create_tables.py to connect to the database and create these tables
3. Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
4. Launch a redshift cluster and create an IAM role that has read access to S3.
5. Add redshift database and IAM role info to dwh.cfg.
6. Test by running create_tables.py and checking the table schemas in your redshift database.

# Build ETL Pipeline

1. Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
2. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
3. Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
4. Delete your redshift cluster when finished.

# Document Process
**Do the following steps in your README.md file:**

- [X] Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
- [X] State and justify your database schema design and ETL pipeline.
- [X] [Optional] Provide example queries and results for song play analysis.

[Here's a guide on Markdown Syntax](https://www.markdownguide.org/basic-syntax/)

## Note
The SERIAL command in Postgres is not supported in Redshift. The equivalent in redshift is IDENTITY(0,1), which you can read more on in the Redshift Create Table Docs.

## Project Rubric
Read the project [rubric](https://review.udacity.com/#!/rubrics/2501/view) before and during development of your project to ensure you meet all specifications.

**REMINDER**: Do not include your AWS access keys in your code when sharing this project!