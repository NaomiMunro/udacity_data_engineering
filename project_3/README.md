### Data Engineering Nanodegree
## Project: Data Warehouse

The premise of this project is that Sparkify, an online music streaming business, want to move their logged data from json files into a cloud based data warehouse. This will allow them to analyse their customers usage and to grow their business.

The Sparkify json data resides in Amazon S3. We build an ETL pipeline that uses the Redshift COPY command to move the data from the json files into staging tables in a Redshift database cluster. The pipeline then creates a star schema of fact and dimension tables and loads these tables from the staging tables.

### Files
create_tables.py  
sql_queries.py  
etl.py  
dwh.cfg - A configuration file containing the locations of the S3 data and the information needed to connect to the redshift cluster.  
redshift.cfg, redshift.py - A script that I used to create and delete the Redshift cluster.  

### ETL Pipeline
The process happens in three parts.

- `create_tables.py` drops any existing tables and then creates staging, fact and dimension tables. The table definitions are located in `sql_queries.py`
- When `etl.py` runs, the `load_staging_tables` procedure copies data from s3 to the staging tables in redshift.
- Also in `etl.py`, `insert_tables` loads data from the staging tables into the fact and dimension tables.

### Database Schema

**Staging Tables**  


```table staging_events
	artist        varchar,
	auth          varchar,
	firstName     varchar,
	gender        varchar,
	itemInSession int,
	lastName      varchar,
	length        float,
	level         varchar,
	location      varchar,
	method        varchar,
	page          varchar,
	registration  float,
	sessionId     int,
	song          varchar,
	status        int,
	ts            bigint,
	userAgent     varchar,
	userId        int

table staging_songs
	artist_id        varchar,
	artist_latitude  float,
	artist_location  varchar,
	artist_longitude float,
	artist_name      varchar,
	duration         float,
	num_songs        int,
	song_id          varchar,
	title            varchar,
	year             int
```


**Fact Table**

```
table songplays
	songplay_id  bigint identity(0,1) PRIMARY KEY,
	start_time   timestamp not null,
	user_id      int not null,
	level        varchar,
	song_id      varchar,
	artist_id    varchar,
	session_id   int,
	location     varchar,
	user_agent   varchar
```

**Dimension tables**
```
table users
	user_id     int PRIMARY KEY,
	first_name  varchar,
	last_name   varchar,
	gender      varchar,
	level       varchar

table songs
	song_id    varchar PRIMARY KEY,
	title      varchar not null,
	artist_id  varchar,
	year       int,
	duration   float not null

table artists
	artist_id  varchar PRIMARY KEY,
	name       varchar not null,
	location   varchar,
	latitude   float,
	longitude  float

table time
	start_time  timestamp PRIMARY KEY,
	hour        int,
	day         int,
	week        int,
	month       int,
	year        int,
	weekday     int
```