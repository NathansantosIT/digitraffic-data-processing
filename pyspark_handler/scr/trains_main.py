import logging
import os
from datetime import datetime
import time

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, explode_outer, concat_ws, sha1, row_number, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType
from pyspark.sql.window import Window

from pyspark_utils import check_timestamp_null, check_date_null

#configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', "digitraffic_live_trains")
trains_table_landing_path = "/data/landing_zone/trains_travel"
trains_table_standard_path = "/data/standard_zone/trains_travel"
trains_table_quality_path = "/data/quality_zone/trains_travel"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Creating SparkSession
spark = (
    SparkSession.builder
        .master("local")
        .appName("TrainsTravel")
        .getOrCreate()
)

def get_trains_schema():
    """
    Get the schema for the trains data
    """
    # Define the schema for the causes field
    causes_schema = StructType([
        StructField("categoryCode", StringType(), True),
        StructField("detailedCategoryCode", StringType(), True),
        StructField("categoryCodeId", LongType(), True),
        StructField("detailedCategoryCodeId", LongType(), True)
    ])

    # Define the schema for the trainReady field
    train_ready_schema = StructType([
        StructField("source", StringType(), True),
        StructField("accepted", BooleanType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Define the schema for the timeTableRows field
    time_table_rows_schema = StructType([
        StructField("stationShortCode", StringType(), True),
        StructField("stationUICCode", LongType(), True),
        StructField("countryCode", StringType(), True),
        StructField("type", StringType(), True),
        StructField("trainStopping", BooleanType(), True),
        StructField("commercialStop", BooleanType(), True),
        StructField("commercialTrack", StringType(), True),
        StructField("cancelled", BooleanType(), True),
        StructField("scheduledTime", StringType(), True),
        # The following fields may not be present in every row in the adhoc type documents
        StructField("liveEstimateTime", StringType(), True),
        StructField("estimateSource", StringType(), True),
        StructField("actualTime", StringType(), True),
        StructField("differenceInMinutes", LongType(), True),
        StructField("causes", ArrayType(causes_schema), True),
        StructField("trainReady", train_ready_schema, True)
    ])

    # Define the main schema for the trains data
    trains_schema = StructType([
        StructField("trainNumber", LongType(), True),
        StructField("departureDate", StringType(), True),
        StructField("operatorUICCode", LongType(), True),
        StructField("operatorShortCode", StringType(), True),
        StructField("trainType", StringType(), True),
        StructField("trainCategory", StringType(), True),
        StructField("commuterLineID", StringType(), True),
        StructField("runningCurrently", BooleanType(), True),
        StructField("cancelled", BooleanType(), True),
        StructField("version", LongType(), True),
        StructField("timetableType", StringType(), True),
        StructField("timetableAcceptanceDate", StringType(), True),
        StructField("timeTableRows", ArrayType(time_table_rows_schema), True)
    ])
    return trains_schema

def read_kafka():
    """
    Read messages from Kafka
    """
    # Read messages from Kafka
    kafka_df = (spark.read.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKER)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "earliest")
                .load()
                )
    # Extract messages as strings
    messages_df = kafka_df.select(col("value").cast("string").alias("jsonValue"),
                                  col("timestamp").alias("kafkaMessageTimestamp")
                                  )
    logger.info("Messages read from Kafka")

    return messages_df

def extract_data_from_kafka(messages_df):
    """
    Extract data from the Kafka messages and explode the timeTableRows array
    """
    trains_schema = get_trains_schema()
    # Extracting data and timestamp of the kafka message
    trains_travel_df = (messages_df
                        .withColumn("data", from_json(col("jsonValue"), trains_schema))
                        .select("data.*","kafkaMessageTimestamp")
                        )
    
    # Transform the trains dataframe to include exploded time table data
    trains_travel_df = (trains_travel_df
                        # Explode the timeTableRows array to create a row for each element
                        .withColumn("timeTableData",explode_outer(trains_travel_df.timeTableRows))
                        .withColumnRenamed("cancelled","trainCancelled")  # Rename the cancelled column to avoid column name conflicts
                        .select("*","timeTableData.*")  # Select all columns including the exploded time table data
                        .drop("timeTableRows","timeTableData")  # Drop the original timeTableRows column
                        )
    logger.info("Data extracted from Kafka messages")

    return trains_travel_df

def generate_quality_report(df: DataFrame) -> None:
    """
    Generate a quality report for the given DataFrame
    """
    # 1. Count the total number of records
    total_train_travel_records = df.count()

    # 2. Check for missing values in critical fields
    missing_trainNumber = df.filter(col("trainNumber").isNull()).count()
    missing_departureDate = df.filter(col("departureDate").isNull()).count()
    missing_scheduledTime = df.filter(col("scheduledTime").isNull()).count()
    missing_stationShortCode = df.filter(col("stationShortCode").isNull()).count()
    missing_type = df.filter(col("type").isNull()).count()

    # 3. Identify duplicates based on unique identifiers (trainNumber, stationShortCode, scheduledTime, type)
    duplicate_count = df.groupBy("trainNumber", "stationShortCode", "scheduledTime", "type").count().filter(col("count") > 1).count()

    # 4. Identify anomalies in date formats:
    expected_timestamp = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
    expected_date = "yyyy-MM-dd"

    schedule_time_errors = check_timestamp_null(df, expected_timestamp, "scheduledTime")
    departure_date_errors = check_date_null(df, expected_date, "departureDate")

    # Build the quality report as a dictionary
    quality_report = {
        "total_train_travel_records": total_train_travel_records,
        "missing_values": {
            "trainNumber": missing_trainNumber,
            "departureDate": missing_departureDate,
            "scheduledTime": missing_scheduledTime,
            "stationShortCode": missing_stationShortCode,
            "type": missing_type
        },
        "duplicate_records": duplicate_count,
        "anomalies": {
            "departureTime_format_errors": departure_date_errors,
            "arrivalTime_format_errors": schedule_time_errors
        },
        "data_quality_generation_time": datetime.now()
    }

    quality_report_df = spark.createDataFrame([quality_report])
    quality_report_df.write.format("delta").mode("append").save(trains_table_quality_path)
    logger.info(f"Quality report written to Delta table at {trains_table_quality_path}")

def standardize_data(df: DataFrame) -> DataFrame:
    """
    Standardize the data and save to the standard zone
    """

    # Concatenate unique ID columns to create a unique identifier for each train travel record

    # Columns chosen: trainNumber, stationShortCode, scheduledTime, type
    # These columns were chosen because a train is not expected to arrive or leave the same station at the same time with the same type
    # This ensures that each train travel record has a unique identifier
    concat_unique_id = concat_ws("_",
                                col("trainNumber"),
                                col("stationShortCode"),
                                col("scheduledTime"),
                                col("type")
                                )
    
    # Add a new column trainTravelId with a unique identifier for each train travel record
    standardized_df = df.withColumn("trainTravelId",sha1(concat_unique_id))

    # Define a window specification to partition by train_travel_id and order by time of received message
    window_spec = Window.partitionBy("trainTravelId").orderBy(col("kafkaMessageTimestamp").desc())

    # Add a row number column to identify the most recent event for each train_travel_id
    standardized_df = (standardized_df
                        .withColumn("rowNum", row_number().over(window_spec))
                        # Filter the dataframe to keep only the most recent event for each train_travel_id
                        .filter(col("rowNum") == 1).drop("rowNum")
                        )
    
    date_format = "yyyy-MM-dd"
    time_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

    # Transform the trains_travel_df to ensure all dates and timestamps are in date or timestamp format
    standardized_df = (standardized_df
                        .withColumn("departureDate", to_date(col("departureDate"), date_format))
                        .withColumn("scheduledTime", to_timestamp(col("scheduledTime"), time_format))
                        .withColumn("actualTime", to_timestamp(col("actualTime"), time_format))
                        .withColumn("liveEstimateTime", to_timestamp(col("liveEstimateTime"), time_format))
                        )
    
    #TODO: Fix obvious data entry errors

    # Check if the Delta table exists
    table_exists = DeltaTable.isDeltaTable(spark, trains_table_standard_path)

    try:
        if table_exists:
            logger.info("Delta table exists, performing merge operation")
            
            # Load the existing Delta table
            delta_table = DeltaTable.forPath(spark, trains_table_standard_path)
            
            # Merge the new data with the existing data in the Delta table
            (delta_table.alias("target")
            .merge(
                standardized_df.alias("source"),
                "target.trainTravelId = source.trainTravelId"  # Condition to match records
                )
            .whenMatchedUpdateAll()  # Update all columns if a match is found
            .whenNotMatchedInsertAll()  # Insert all columns if no match is found
            .execute()
            )
            logger.info("Merge operation completed successfully")
        else:
            logger.info("Delta table does not exist, creating a new Delta table")
            
            # If the table does not exist, create a new Delta table
            (standardized_df
            .write
            .format("delta")
            .mode("overwrite")  # Overwrite mode to create the table
            .save(trains_table_standard_path)
            )
            logger.info(f"Delta table created at path: {trains_table_standard_path}")

    #TODO: Handle specific exceptions
    except Exception as e:
        logger.error(f"Table not updated {e}")

def main() -> None:
    """Main function to read data from Kafka, extract and standardize the data"""

    logger.info("Starting data processing...")
    time.sleep(20)  # Sleep for 20 seconds to garantuee that messages are available in Kafka

    # Read messages from Kafka
    messages_df = read_kafka()

    # Extract data from Kafka messages, exploding arrays to facilitate further processing
    trains_travel_df = extract_data_from_kafka(messages_df)

    # Generate quality report and save to the quality zone
    generate_quality_report(trains_travel_df)

    # Save the raw data to the landing zone
    messages_df.write.format("delta").mode("append").save(trains_table_landing_path)

    # Standardize the data and save to the standard zone
    standardize_data(trains_travel_df)

    # Stop the Spark session
    spark.stop()
    logger.info("Data processing completed successfully")

if __name__ == "__main__":
    main()