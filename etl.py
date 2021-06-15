from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
from pyspark.sql import functions as F


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    input_songs_data = f"{input_data}/songs/A/*/*/*.json"

    # read song data file
    df = spark.read.json(input_songs_data)

    # extract columns to create songs table
    songs_table = (
        df.filter(df.song_id.isNotNull())
        .dropDuplicates(subset=["song_id"])
        .select("song_id", "title", "artist_id", "year", "duration", "artist_name")
    )

    # write songs table to parquet files partitioned by year and artist
    output_songs_data = f"{output_data}/songs/"
    songs_table.repartition("year", "artist_name").write.partitionBy(
        "year", "artist_name"
    ).mode("overwrite").parquet(output_songs_data)

    # extract columns to create artists table
    artists_table = (
        df.filter(df.artist_id.isNotNull())
        .dropDuplicates(subset=["artist_id"])
        .withColumnRenamed("artist_name", "name")
        .withColumnRenamed("artist_location", "location")
        .withColumnRenamed("artist_latitude", "latitude")
        .withColumnRenamed("artist_longitude", "longitude")
        .select("artist_id", "name", "location", "latitude", "longitude")
    )

    # write artists table to parquet files
    output_artists_data = f"{output_data}/artists/"
    artists_table.repartition("name").write.partitionBy("name").mode(
        "overwrite"
    ).parquet(output_artists_data)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    input_log_data = f"{input_data}/logs/"

    # read log data file
    df = spark.read.json(input_log_data)
    # rename columns
    df = (
        df.withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("itemInSession", "item_in_session")
        .withColumnRenamed("lastName", "last_name")
        .withColumnRenamed("sessionId", "session_id")
        .withColumnRenamed("userAgent", "user_agent")
        .withColumnRenamed("userId", "user_id")
    )  # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = (
        df.filter(df.user_id.isNotNull())
        .dropDuplicates(subset=["user_id"])
        .withColumn("first_name_letter", substring("first_name", 1, 1))
        .select(
            "user_id", "first_name", "first_name_letter", "last_name", "gender", "level"
        )
    )
    # write users table to parquet files
    output_users_data = f"{output_data}/users/"
    users_table.repartition("first_name_letter").write.partitionBy(
        "first_name_letter"
    ).mode("overwrite").parquet(output_users_data)

    # extract columns to create time table
    time_table = (
        df.dropDuplicates(subset=["ts"])
        .withColumn("start_time", F.from_unixtime(F.col("ts") / 1000))
        .withColumn("start_time", F.to_timestamp("start_time"))
        .withColumn("hour", F.hour("start_time"))
        .withColumn("day", F.dayofyear("start_time"))
        .withColumn("week", F.weekofyear("start_time"))
        .withColumn("month", F.month("start_time"))
        .withColumn("year", F.year("start_time"))
        .withColumn("weekday", F.dayofweek("start_time"))
        .select("start_time", "hour", "day", "week", "month", "year", "weekday")
    )

    # write time table to parquet files partitioned by year and month
    output_time_table = f"{output_data}/time/"
    time_table.repartition("year", "month").write.partitionBy("year", "month").mode(
        "overwrite"
    ).parquet(output_time_table)

    # read in song data to use for songplays table
    songs_processed_data = f"{output_data}/songs/"
    song_df = spark.read.parquet(songs_processed_data)

    # read in artists data to use for songplays table
    artists_processed_data = f"{output_data}/artists/"
    artist_df = spark.read.parquet(artists_processed_data)

    # join songs and artists tables together
    songs_artists_df = song_df.join(
        artist_df, song_df.artist_id == artist_df.artist_id, "inner"
    ).select(
        song_df.song_id,
        artist_df.artist_id,
        song_df.title,
        song_df.duration,
        artist_df.name,
    )

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(
        songs_artists_df,
        (songs_artists_df.name == df.artist)
        & (songs_artists_df.title == df.song)
        & (songs_artists_df.duration == df.length),
        "left",
    )

    songplays_table = (
        songplays_table.withColumn("timestamp", F.from_unixtime(F.col("ts") / 1000))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("year", F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .select(
            "user_id",
            "level",
            "song_id",
            "artist_id",
            "session_id",
            "location",
            "user_agent",
            "timestamp",
            "year",
            "month",
        )
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_processed_data = f"{output_data}/songplays/"
    songplays_table.repartition("year", "month").write.partitionBy(
        "year", "month"
    ).mode("overwrite").parquet(songplays_processed_data)


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-de-files/raw"
    output_data = "s3://udacity-de-files/processed"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
