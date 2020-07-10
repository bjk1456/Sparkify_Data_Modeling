import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, row_number


#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Connect to Spark instance

    Returns:
    spark -- The entry point to Spark SQL
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("SparkifyAnalysis") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Obtain songs from JSON files and create artists and songs tables from them

    Parameters:
    spark       -- The entry point to Spark SQL
    input_data  -- The root directory containing the JSON songs files
    output_data -- The directory to contain the parquet files
    """
    # get filepath to song data file
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id","title","artist_id","year","duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, "songs_table/"), mode='overwrite', partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select(["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"])
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists_table/"), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Obtain logs from JSON files, filter such that page=NextSong, and create users, time, and songplays tables from them

    Parameters:
    spark       -- The entry point to Spark SQL
    input_data  -- The root directory containing the JSON logs files
    output_data -- The directory to contain the parquet files
    """
    # get filepath to log data file
    log_data = 's3a://udacity-dend/log-data/*/*/*.json'
    
    # read log data file
    df = spark.read.json(log_data)
    print("Reading log data")
    
    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")

    # extract columns for users table
    users_table = df.select(["userId","firstName","lastName","gender","level"])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users_table/"), mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    
    # create datetime column from original timestamp column
    get_datetime = udf()

    df = (df
        .withColumn("start_time", from_unixtime(df["ts"]/1000))
        .withColumn("month", month(from_unixtime(df["ts"]/1000)))
        .withColumn("year", year(from_unixtime(df["ts"]/1000)))
        )
    
    # extract columns to create time table
    time_table = (df
       .withColumn("hour", hour(from_unixtime(df["ts"]/1000)))
       .withColumn("day", dayofmonth(from_unixtime(df["ts"]/1000)))
       .withColumn("week", weekofyear(from_unixtime(df["ts"]/1000)))
       .withColumn("month", month(from_unixtime(df["ts"]/1000)))
       .withColumn("year", year(from_unixtime(df["ts"]/1000)))
      )

    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year","month"])
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet("s3a://spark-startup/parq/songs_table/")
    song_df = song_df.drop('year')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner')
    
    window = Window.orderBy(col('start_time'))
    songplays_table = songplays_table.withColumn('songplay_id', row_number().over(window))
     
    # write songplays table to parquet files partitioned by year and month
    songplays_table.select(['songplay_id','start_time','userId','level','song_id','artist_id','sessionId','location',
                            'userAgent','year','month']).write \
                    .parquet(os.path.join(output_data, "songplays_table/"), mode='overwrite', partitionBy=["year","month"])

def main():
    """
    Obtain the raw data and thenncreate a star schema optimized for queries on song play analysis
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-startup/parq/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()


if __name__ == "__main__":
    main()
