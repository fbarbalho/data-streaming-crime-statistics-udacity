import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# TODO Create a schema for incoming resources
# Based on pollice-department-calls-for-service.json
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])


def run_spark_job(spark):
    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    df = spark.readStream.\
        .format('kafka')\
        .option('kafka.bootstrap.servers', 'localhost:9092')\
        .option('subscribe', 'police.department.calls.event')\
        .option('startingOffsets', 'earliest')\
        .option('maxOffsetsPerTrigger', '200')\
        .option('stopGracefullyOnShutdown', 'true')\
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Convert value to String using CAST function
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("service_table"))\
        .select("service_table.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table \
        .select( "original_crime_type_name","disposition") \
        .distinct()

    # count the number of original crime type
    agg_df = distinct_table \
        .dropna() \
        .select("original_crime_type_name") \
        .withWatermark("call_datetime", "25 minutes") \
        .groupby("original_crime_type_name") \
        .agg({"original_crime_type_name" : "count"}) \
        .orderBy("count(original_crime_type_name)", ascending=False)
        .count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    query = agg_df\
        .writeStream\
        .trigger(processingTime="10 seconds")\
        .outputMode("Complete")\
        .format("console")\
        .queryName("Progress Report")\
        .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()

    # Get radio_code json file
    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # Rename disposition_code column
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df\
        .join(radio_code_df, "disposition")\
        .writeStream\
        .trigger(processingTime="10 seconds")\
        .outputMode("Complete")\
        .format("console")\
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    # SET spark ui port to 3000
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
