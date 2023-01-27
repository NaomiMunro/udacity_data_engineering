### Data Engineering Nanodegree
## Project 4: Data Lake

Sparkify's userbase has continued to grow and they now want to move their data to a data lake. The Sparkify song data and and user activity logs reside in JSON files in Amazon S3. In this project we build an ETL pipeline that extracts the data from the S3 files, processes it and then writes the processed data to dimension and fact tables in a data lake that is stored back in S3 as parquet files.

### Files
`dl.cfg` - A configuration file containing the AWS credentials  
`etl.py` - The extract, transform and load pipeline  
`data_lake.ipynb` - An iPython/Jupyter notebook that I used to design and test the etl script  
`README.md` - This file  

### How to run
This script can run on any computer that has pyspark installed, including Amazon EMR.  
1. Add your AWS credentials to `dl.cfg`  
2. Install pyspark if it is not already installed: `pip install pyspark`  
3. Run `python etl.py`  

### ETL Pipeline
The etl script does its work in three main procedures.  
`create_spark_session` opens a spark session that handles the connection to the Spark server and to Amazon S3.  
`process_song_data` loads song metadata from JSON files residing in S3, processes it into dimension tables for songs and artists, then writes parquet files back to a different s3 bucket.  
The third procedure, `process_log_data`, loads user event log json data from S3. The data is processed into two dimension tables (users and time) - and a fact table (songplays). These are then written to parquet files in s3. 

### Schema
**Dimension tables**  
*Songs table*
```
root
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- year: long (nullable = true)
 |-- duration: double (nullable = true)
```

*Artists table*
```
root
 |-- artist_id: string (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_latitude: double (nullable = true)
 |-- artist_longitude: double (nullable = true)
```

*Users table*
```
root
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
 |-- user_id: string (nullable = true)
```

*Time table*
```
root
 |-- start_time: timestamp (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- weekday: integer (nullable = true)
```

**Fact table**  
*Songplays table*
```
root
 |-- start_time: timestamp (nullable = true)
 |-- user_id: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- songplay_id: long (nullable = false)
```