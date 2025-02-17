from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import schema_of_json, to_timestamp, to_date, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType

def check_timestamp_null(df: DataFrame, expected_timestamp: str, timestamp_column:str) -> int:
    """
    Check for null values in the timestamp column after converting to the expected timestamp format.

    Parameters:
    df (DataFrame): The input DataFrame.
    expected_timestamp (str): The expected timestamp format.
    timestamp_column (str): The name of the timestamp column to check.

    Returns:
    int: The count of rows where the timestamp column is not null but the conversion to the expected format is null.
    """
    errors_count = (
        df.withColumn("columnExpected", to_timestamp(col("scheduledTime"), expected_timestamp))
          .filter((col(timestamp_column).isNotNull()) & (col("columnExpected").isNull()))
          .count()
    )
    return errors_count

def check_date_null(df: DataFrame, expected_date: str, date_column: str) -> int:
    """
    Check for null values in the date column after converting to the expected date format.

    Parameters:
    df (DataFrame): The input DataFrame.
    expected_date (str): The expected date format.
    date_column (str): The name of the date column to check.

    Returns:
    int: The count of rows where the date column is not null but the conversion to the expected format is null.
    """
    errors_count = (
        df.withColumn("columnExpected", to_date(col(date_column), expected_date))
          .filter((col(date_column).isNotNull()) & (col("columnExpected").isNull()))
          .count()
    )
    return errors_count

def infer_json_schema(spark: SparkSession, df: DataFrame, json_col: str) -> StructType:
    """
    Infer the schema from a JSON column in a DataFrame. This is useful to convert
    JSON strings into a structured format and ease further transformations. Note that
    this method is recommended only when you are certain that all messages follow the same schema.

    Parameters:
        df (DataFrame): The DataFrame containing the JSON data.
        json_col (str): The name of the column that contains JSON strings.
        spark (SparkSession): An active SparkSession instance.

    Returns:
        StructType: The inferred schema for the JSON data.
    
    Raises:
        ValueError: If the DataFrame is empty or the JSON column contains no data.
    """
    # Take one sample JSON message for schema inference
    sample_rows = df.select(json_col).limit(1).collect()
    
    if not sample_rows:
        raise ValueError("The DataFrame is empty. Cannot infer schema from an empty DataFrame.")

    # Get the JSON string from the sample row
    sample_json_str = sample_rows[0][json_col]
    
    # Infer schema from the sample JSON string.
    # Note: We use spark.range(1) to create a dummy DataFrame for schema inference.
    inferred_schema = spark.range(1) \
        .select(schema_of_json(sample_json_str).alias("schema")) \
        .collect()[0]["schema"]
    
    return inferred_schema