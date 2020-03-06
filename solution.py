import argparse
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F


def convert_csv_to_parquet(spark: SparkSession, input_location: str, output_location: str):
    spark.read.option("header", "true").csv(input_location).write.parquet(output_location)


def create_views(spark: SparkSession, input_location: str):
    df_parq = spark.read.option("header", "true").parquet(input_location)
    df_parq.createOrReplaceTempView("weather_report")


def run_transformations(spark: SparkSession, input_location: str):
    create_views(spark, input_location)
    weather_df = spark.sql("select * from weather_report")
    print(find_hottest_day(weather_df))
    print(find_hottest_day_temp(weather_df))
    print(find_hottest_day_region(weather_df))


def find_hottest_row_dataframe(weather_df: DataFrame):
    hottest_day_df = weather_df.withColumn("ScreenTemperature", col("ScreenTemperature").cast("float")). \
        select(F.max("ScreenTemperature").alias("Temperature")). \
        join(weather_df, col("ScreenTemperature") == col("Temperature"), "inner"). \
        select("ObservationDate", "Temperature", "Region").collect()[0]
    return hottest_day_df


def find_hottest_day(weather_df: DataFrame):
    hottest_row = find_hottest_row_dataframe(weather_df)
    return hottest_row.ObservationDate


def find_hottest_day_temp(weather_df: DataFrame):
    hottest_row = find_hottest_row_dataframe(weather_df)
    return hottest_row.Temperature


def find_hottest_day_region(weather_df: DataFrame):
    hottest_row = find_hottest_row_dataframe(weather_df)
    return hottest_row.Region


if __name__ == "__main__":
    spark_session = (
        SparkSession.builder
            .master("local[2]")
            .appName("DataTest")
            .config("spark.executorEnv.PYTHONHASHSEED", "0")
            .getOrCreate())
    parser = argparse.ArgumentParser(description='WeatherTest')
    parser.add_argument('--input_data', required=False,
                        default="C:\\Users\\NishanthSankaralinga\\Desktop\\Test_data\\weather.20160201.csv")
    parser.add_argument('--output_data', required=False,
                        default="C:\\Users\\NishanthSankaralinga\\Desktop\\Output_data\\output.parquet")
    args = vars(parser.parse_args())
    convert_csv_to_parquet(spark_session, args['input_data'], args['output_data'])
    run_transformations(spark_session, args['output_data'])
