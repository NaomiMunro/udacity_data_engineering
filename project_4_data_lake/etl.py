import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Process song and artist data 

        Parameters:
            none
        Returns:
            SparkSession object
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Process song and artist data and record it in s3 parquet files

        Parameters:
            spark: SparkSession object
            input_data: Source s3 location
            output_data: Target s3 location
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').mode('overwrite') \
        .parquet(os.path.join(output_data + 'songs_table.parquet'))
    
    print('*** songs_table.parquet written to s3 ***')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data + 'artists_table.parquet'))

    print('*** artists_table.parquet written to s3 ***')

def process_log_data(spark, input_data, output_data):
    """ Process song log data and record it in s3 parquet files

        Parameters:
            spark: SparkSession object
            input_data: Source s3 location
            output_data: Target s3 location
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/2018/11/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(df.userId.alias('user_id'), df.firstName.alias('first_name'), df.lastName.alias('last_name'), 'gender', 'level').distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data + 'users_table.parquet'))
    
    print('*** users_table.parquet written to s3 ***')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp('ts'))

    # extract columns to create time table
    df = df.withColumn('hour', hour('start_time')) \
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', dayofweek('start_time')) 
    time_table = df.select('start_time', 'weekday', 'year', 'month', 'week', 'day', 'hour').distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').mode('overwrite') \
        .parquet(os.path.join(output_data, 'time_table.parquet'))
    
    print('*** time_table.parquet written to s3 ***')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs_table.parquet'))
    artist_df = spark.read.parquet(os.path.join(output_data, 'artists_table.parquet'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,
                              on=[df.song == song_df.title,
                                  df.length == song_df.duration],
                              how='inner') \
                        .join(artist_df,
                              on=[df.artist == artist_df.artist_name,
                                  song_df.artist_id == artist_df.artist_id],
                              how='inner') \
                        .drop(song_df.artist_id) \
                        .drop(song_df.year) \
                        .select('start_time', df.userId.alias('user_id'), 'level', 'song_id',
                                'artist_id', df.sessionId.alias('session_id'), 'location',
                                df.userAgent.alias('user_agent'), df.year, df.month) \
                        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').mode('overwrite') \
        .parquet(os.path.join(output_data, 'songplays_table.parquet'))
    
    print('*** songs_table.parquet written to s3 ***')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://naomi-udacity-data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
