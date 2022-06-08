import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F
import sql_queries as sq

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function will process the song data by reading the json data, processing and creating song, artist table based on it.
    """
    # get filepath to song data file
    song_df = spark.read.json(input_data + "/song_data/*/*/*/*.json")
    song_df.createOrReplaceTempView("songs_db")

    # extract columns to create songs table
    songs_table = spark.sql(sq.songs_q)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(output_data + "/song_table.parquet")

    # extract columns to create artists table
    artists_table =  spark.sql(sq.artists_q)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "/artists_table.parquet")

def format_ts(value):
    """
    This is an udf function which reads the value which is unix epoch time and convert that into timestamp in the defined format.
    It returns a string as output.
    """
    return datetime.fromtimestamp(value/1000).strftime("%Y-%m-%d %H:%M:%S")

def process_log_data(spark, input_data, output_data):
    """
    The log data containing the user events are processed, to create user table. For the user table, duplication is removed by running an sql query.
    For the time table, first timestamp column is extracted and based on it all the required informations are collected.
    For the songplays fact table, a sql select with join of both time and songs table is done, and joined based on song_title, and duration of the song.
    """
    # For local: spark.read.json(input_data + "/log-data/*.json")
    log_e_df = spark.read.json(input_data + "/log_data/*/*/*.json")
    log_e_df.createOrReplaceTempView("logs_db")
    
    # filter by actions for song plays
    logs_df = spark.sql(sq.logs_q)
    logs_df.createOrReplaceTempView("nextPlay_logs_db")

    # ctx handling the level changes to have completely unique user_ids
    users_table = spark.sql(sq.users_q)
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "/users_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: format_ts(x))
    logs_df = logs_df.withColumn("timestamp", get_timestamp(logs_df.ts))
    # Cast the String timestamp to timestamp data type.
    logs_df = logs_df.withColumn('timestamp', logs_df.timestamp.cast("timestamp"))


    # extract columns to create time table. Extract all the required time related information.
    logs_df = logs_df.withColumn("year", F.year("timestamp")) \
             .withColumn("month", F.month("timestamp")) \
             .withColumn("dayofmonth", F.dayofmonth("timestamp")) \
             .withColumn("hour", F.hour("timestamp")) \
             .withColumn("weekofyear", F.weekofyear("timestamp")) \
             .withColumn("weekday", F.dayofweek("timestamp"))

    # Creating a temp view for time_events
    logs_df.createOrReplaceTempView("time_db")
    time_table = spark.sql(sq.time_q)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "/time_table.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/song_table.parquet")
    song_df.createOrReplaceTempView("song_table")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(sq.songplays_q)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "/songplays_table.parquet")


def main():
    spark = create_spark_session()
    # Input and output data
    input_data = config.get("files", "input_data")
    output_data = config.get("files", "output_data")
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
