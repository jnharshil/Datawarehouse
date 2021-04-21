import configparser
import pandas as pd
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, DecimalType as Dec

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

SongDataSchema = R([
    Fld("artist_id",Str()),
    Fld("artist_latitude",Dec()),
    Fld("artist_location",Str()),
    Fld("artist_longitude",Dec()),
    Fld("artist_name",Str()),
    Fld("duration",Str()),
    Fld("num_songs",Int()),
    Fld("song_id",Str()),
    Fld("title",Str()),
    Fld("year",Int()),
        ])


def create_spark_session():
     """
    creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
     """
    read the song data from the pecified input path transforms data and create parquet files
    """
    # read song data file
    df = spark.read.json('s3a://udacity-dend/song_data/*/*/*/*.json',schema = SongDataSchema)
    df.printSchema()
    
    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    
#     # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs_table.parquet", mode="overwrite")
    
#     # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
#     # write artists table to parquet files
    artists_table.write.parquet(output_data  + "artists_table.parquet",mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    read the log data from the pecified input path transforms data and create parquet files
    """
    
    LogDataSchema = R([
    Fld("artist",Str()),
    Fld("auth",Str()),
    Fld("firstName",Str()),
    Fld("gender",Str()),
    Fld("itemInSession",Int()),
    Fld("lastName",Str()),
    Fld("length",Dbl()),
    Fld("level",Str()),
    Fld("location",Str()),
    Fld("method",Str()),
    Fld("page",Str()),
    Fld("registration",Dbl()),
    Fld("sessionId",Int()),
    Fld("song",Str()),
    Fld("status",Int()),
    Fld("ts",Str()),
    Fld("userAgent",Str()),
    Fld("userId",Str()),
        ])
    # get filepath to log data file
    log_data = 's3a://udacity-dend/log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data , schema = LogDataSchema)

    
#     # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')


#     # extract columns for users table    
    user_table = df.select("artist","firstName","lastName","gender","level").drop_duplicates()
    
#     # write users table to parquet files
    user_table.write.parquet(output_data  + "user_table.parquet",mode="overwrite")

#     # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp((int(x)/1000)).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

# create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((int(x)/1000)).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))

    

    
#     # extract columns to create time table
    time_table = df.select( 'timestamp',
    hour('datetime').alias('hour'),
    dayofmonth('datetime').alias('day'),
    weekofyear('datetime').alias('week'),
    month('datetime').alias('month'),
    year('datetime').alias('year'),
    date_format('datetime', 'F').alias('weekday'))

    
#     # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data  + "time_table.parquet",mode="overwrite")

#     # read in song data to use for songplays table
    song_df = spark.read.json('s3a://udacity-dend/song_data/A/A/*/*.json',schema = SongDataSchema)

#     # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), 'left_outer').select(
            df.timestamp,
            col("userId").alias('user_id'),
            df.level,song_df.song_id,
            song_df.artist_id,
            col("sessionId").alias("session_id"),
            df.location,
            col("useragent").alias("user_agent"),
            year('datetime').alias('year'),month('datetime').alias('month')).drop_duplicates()
#     # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data  + "songplays_table.parquet",mode="overwrite")


def main():
    """is the main function 
    creates a spark session
    and calls the other function to preform ETL."""
    
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "udacity-out/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
