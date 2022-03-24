
# Purpose of this database in context of the startup- Sparkify and their analytical goals.
The data warehouse is used to extract JSON format data available in AWS S3 for getting key insights by analytics team on songs play such as what songs their users are listening to. 


# Justification of database schema design and ETL pipeline
An ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.
# Example queries and results for song play analysis.
> select count(*) as total_user from dim_user

| total_user |
|------------|
|    105     |


> select count(*) as total_song from dim_song

| total_song |
|------------|
|   14896    |

> select count(*) as total_artist from dim_artist

| total_artist |
--------------
|    10025     |


> select count(*) as total_time from dim_time

| total_time |
------------
|    8023    |


> select count(*) as total_songplay from fact_songplay

| total_songplay |
|----------------|
|      326       |

> select count(*) as stag_event from staging_events

|stag_event
----------
8056


> select count(*) as stag_song from staging_songs

|stag_song
-------------
14896


# Steps:

1. Import all the necessary libraries
2. Write the configuration of AWS Cluster, store the important parameter in some other file
3. Configuration of boto3 which is an AWS SDK for Python
4. Using the bucket, can check whether files log files and song data files are present
5. Create an IAM User Role, Assign appropriate permissions and create the Redshift Cluster
6. Get the Value of Endpoint and Role for put into main configuration file
7. Authorize Security Access Group to Default TCP/IP Address
8. Launch database connectivity configuration
9. Go to Terminal write the command "python create_tables.py" and then "etl.py"
10. Should take around 4-10 minutes in total
11. Then you go back to jupyter notebook to test everything is working fine
12. I counted all the records in my tables
13. Now can delete the cluster, roles and assigned permission

# References:
- [Rubric Review](https://review.udacity.com/#!/rubrics/2501/view)
Here are some resources to learn further:

Top 8 Best Practices for High-Performance ETL Processing Using Amazon Redshift
https://aws.amazon.com/blogs/big-data/top-8-best-practices-for-high-performance-etl-processing-using-amazon-redshift/

How I built a data warehouse using Amazon Redshift and AWS services in record time

https://aws.amazon.com/blogs/big-data/how-i-built-a-data-warehouse-using-amazon-redshift-and-aws-services-in-record-time/

Redshift ETL: 3 Ways to load data into AWS Redshift

https://panoply.io/data-warehouse-guide/redshift-etl/

2X Your Redshift Speed With Sortkeys and Distkeys

https://www.sisense.com/blog/double-your-redshift-performance-with-the-right-sortkeys-and-distkeys/


- Please note that Redshift does not enforce unique, primary-key, and foreign-key constraints. Even though they are informational only, the query optimizer uses those constraints to generate more efficient query plans.

Ref:
https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-defining-constraints.html

http://www.sqlhaven.com/amazon-redshift-what-you-need-to-think-before-defining-primary-key/

Include an ER Diagram to show how the different tables are connected.
You can make use of online tools like 
https://www.lucidchart.com/

Refer good READMEs:

https://github.com/matiassingers/awesome-readme

https://bulldogjob.com/news/449-how-to-write-a-good-readme-for-your-github-project
https://medium.com/@meakaakka/a-beginners-guide-to-writing-a-kickass-readme-7ac01da88ab3

Here's an excellent guide on writing beautiful Python code with PEP8: 

https://realpython.com/python-pep8/

Well-formatted SQL statements are important as well. Here are some references:

https://www.sqlstyle.guide/
https://gist.github.com/fredbenenson/7bb92718e19138c20591