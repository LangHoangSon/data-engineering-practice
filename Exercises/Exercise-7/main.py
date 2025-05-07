from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, desc, to_date, date_format, expr, row_number, when, year, lit
)
from pyspark.sql.window import Window
import zipfile
import os
import datetime


def extract_zip_files(data_dir):
    for filename in os.listdir(data_dir):
        if filename.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(data_dir, filename), 'r') as zip_ref:
                zip_ref.extractall(data_dir)


def load_data(spark, data_dir):
    return spark.read.csv(
        f"{data_dir}/*.csv",
        header=True,
        inferSchema=True
    )


def average_trip_duration_per_day(df):
    return (
        df.withColumn("date", to_date("start_time"))
          .groupBy("date")
          .agg(avg("tripduration").alias("average_tripduration"))
    )


def trips_per_day(df):
    return (
        df.withColumn("date", to_date("start_time"))
          .groupBy("date")
          .agg(count("*").alias("num_trips"))
    )


def most_popular_start_station_per_month(df):
    df_month = df.withColumn("month", date_format("start_time", "yyyy-MM"))
    window_spec = Window.partitionBy("month").orderBy(desc("count"))
    return (
        df_month.groupBy("month", "from_station_name")
                .agg(count("*").alias("count"))
                .withColumn("rank", row_number().over(window_spec))
                .filter(col("rank") == 1)
                .drop("rank")
    )


def top_3_stations_last_2_weeks(df):
    df = df.withColumn("date", to_date("start_time"))
    max_date = df.agg({"date": "max"}).collect()[0][0]
    cutoff = max_date - datetime.timedelta(days=13)
    recent_df = df.filter(col("date") >= lit(cutoff))
    window_spec = Window.partitionBy("date").orderBy(desc("count"))

    return (
        recent_df.groupBy("date", "from_station_name")
                 .agg(count("*").alias("count"))
                 .withColumn("rank", row_number().over(window_spec))
                 .filter(col("rank") <= 3)
                 .drop("rank")
    )


def gender_trip_duration_comparison(df):
    return (
        df.groupBy("gender")
          .agg(avg("tripduration").alias("avg_duration"))
          .filter(col("gender").isin("Male", "Female"))
    )


def top_10_ages_longest_and_shortest(df):
    current_year = datetime.datetime.now().year
    df_age = df.withColumn("age", current_year - col("birthyear"))
    df_age = df_age.filter(col("age").isNotNull())

    longest = df_age.orderBy(desc("tripduration")).select("age", "tripduration").limit(10)
    shortest = df_age.orderBy(col("tripduration")).select("age", "tripduration").limit(10)

    return longest, shortest


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    data_dir = "data"
    reports_dir = "reports"

    os.makedirs(reports_dir, exist_ok=True)
    extract_zip_files(data_dir)
    df = load_data(spark, data_dir).cache()

    average_trip_duration_per_day(df).write.csv(f"{reports_dir}/average_trip_duration_per_day", header=True, mode="overwrite")
    trips_per_day(df).write.csv(f"{reports_dir}/trips_per_day", header=True, mode="overwrite")
    most_popular_start_station_per_month(df).write.csv(f"{reports_dir}/popular_start_station_per_month", header=True, mode="overwrite")
    top_3_stations_last_2_weeks(df).write.csv(f"{reports_dir}/top3_stations_last_2_weeks", header=True, mode="overwrite")
    gender_trip_duration_comparison(df).write.csv(f"{reports_dir}/gender_trip_duration_comparison", header=True, mode="overwrite")

    longest, shortest = top_10_ages_longest_and_shortest(df)
    longest.write.csv(f"{reports_dir}/top10_ages_longest", header=True, mode="overwrite")
    shortest.write.csv(f"{reports_dir}/top10_ages_shortest", header=True, mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()
