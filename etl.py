import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, to_timestamp
from pyspark.sql.types import DateType, TimestampType, IntegerType

DEBUG = False

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark

    
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    #song_data = input_data + "song_data/A/A/A/*.json"
    song_data = input_data + "song_data/*/*/*/*.json"
    
    DEBUG and print("Reading song data files from", song_data)
    
    # read song data file
    df = spark.read.json(song_data)

    DEBUG and print("Creating and persisting songs table")
    
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").where(col("song_id").isNotNull()).dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(["year", "artist_id"]).parquet(output_data + "songs/", mode='overwrite')

    DEBUG and print("Creating and persisting artists table")
    
    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").where(col("artist_id").isNotNull()).dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table = artists_table.write.parquet(output_data + "artists/", mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    song_data = input_data + "song_data/*/*/*/*.json"
    #song_data = input_data + "song_data/A/A/A/*.json"

    DEBUG and print("Reading log data files from", log_data)
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong') \
            .where(df.ts.isNotNull()) \
            .withColumn("userId", df["userId"].cast(IntegerType())) \
            .withColumn("sessionId", df["sessionId"].cast(IntegerType()))

    DEBUG and print("Preparing users table")
    
    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").where(col("userId").isNotNull()).dropDuplicates(['userId'])
    
    DEBUG and print("Creating and persisting users table")
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", mode='overwrite')
    
    DEBUG and print("Creating and persisting time table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour(df.start_time)) \
                    .withColumn("day", dayofmonth(df.start_time)) \
                    .withColumn("week", weekofyear(df.start_time)) \
                    .withColumn("month", month(df.start_time)) \
                    .withColumn("year", year(df.start_time)) \
                    .withColumn("weekday", dayofweek(df.start_time)) \
                    .select("start_time", "hour", "day", "week", "month", "year", "weekday") \
                    .dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(["year", "month"]).parquet(output_data + "times/", mode='overwrite')

    DEBUG and print("Creating and persisting songplays table")
    
    # read in song data to use for songplays table
    song_df = spark.read.json(song_data).select("song_id", "title", "artist_id", "artist_name")    
    action_df = df.select("start_time", "userId", "level", "sessionId", "location", "userAgent", "artist", "song")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = action_df.join(song_df, (action_df.artist == song_df.artist_name) & (action_df.song == song_df.title)) \
                                .select(monotonically_increasing_id().alias("songplay_id"), "start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent") \
                                .withColumn("month", month(df.start_time)) \
                                .withColumn("year", year(df.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(["year", "month"]).parquet(output_data + "songplays/", mode='overwrite')


def main():
    DEBUG and print("Creating Spark Session")
    
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://fawkesbucketdemoo/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    DEBUG and print("Fin.")


if __name__ == "__main__":
    main()
