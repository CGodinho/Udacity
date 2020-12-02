import logging
import json
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

# Create a schema for incoming resources
schema = StructType([StructField("crime_id", StringType(), True),
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
                     StructField("common_location", StringType(), True)])

def run_spark_job(spark):

    print('>>> ---------------------- SETUP STREAM ------------------------')
    
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sfpd.call.log") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "100") \
        .option("maxRatePerPartition", "200") \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    print('>>> ----------------- PRINT INITIAL SCHEMA --------------------')
    df.printSchema()

    
    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")
      
    print('>>> ------------- PRINT SERVICE TABLE SCHEMA -----------------')
    service_table.printSchema()

    # select original_crime_type_name and disposition
    distinct_table = service_table \
        .select("original_crime_type_name", psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time"), "disposition") \
        .withWatermark("call_date_time", "1 hour")

    print('>>> ----------------- PRINT DIST SCHEMA ----------------------')
    distinct_table.printSchema()

    # count the number of original crime type
    agg_df = distinct_table \
        .groupBy("original_crime_type_name", psf.window("call_date_time", "1 hour")) \
        .agg({'original_crime_type_name':'count'})

    # Q1. Submit a screen shot of a batch ingestion of the aggregation
    # write output stream
    print('>>> --------------- WRITE OUTPUT STREAM --------------------')
    query = agg_df \
        .writeStream \
        .queryName("write_stream") \
        .outputMode("complete") \
        .format("console") \
        .start()

    # Attach a ProgressReporter
    query.awaitTermination()
      
    print('>>> -------------------- RADIO CODE  ----------------------')
    
    # Get the right radio code json path, point ot file
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    print('----------------- RADIO CODE JSON SCHEMA --------------------')
    radio_code_df.printSchema()

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code
    # Rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    print('>>> ----------------- JOINING --------------------')
      
    # Join on disposition column
    join_query = agg_df \
        .queryName("joining_call_with_radio") \
        .join(radio_code_df, "disposition") \
        .writeStream \
        .format("console") \
        .start()

    join_query.awaitTermination()

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()