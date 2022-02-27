1. All the files in subdirectories in songs_data and log_data are traversed with python glob module.
2. Pandas Dataframe has been created with filtered data
3. As a part of star schema, Fact/ Dimension tables are created
4. Appropriate constraints in table fields has taken care such as Primary Key, NOT NULL.
5. Appropriate data type have been chosen for each fields for tables.
6. Timestamp column is typecasted to datetime to convert python.int type (Postgresql bigint) to datetime.
7. Day, week, month, year etc. extracted and saved in time table.
8. ETL queries created and run to populated Dimension tables
9. To populate fact table: songsplay (inner) join query with songs table and artist table created with artist_id as common fields between two tables.
10. Run test.ipynb for sanity checks constraints and data availability in the tables.