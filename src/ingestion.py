from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)

def read_orders_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read orders data from a CSV file into a Spark DataFrame.

    Parameters:
    spark (SparkSession): The Spark session to use for reading the data.
    file_path (str): The path to the CSV file containing orders data.

    Returns:
    DataFrame: A Spark DataFrame containing the orders data.
    
    Raises:
    FileNotFoundError: If the specified file path does not exist.
    Exception: If there's an error reading the CSV file.
    """
    try:
        logger.info(f"Reading CSV file from: {file_path}")
        orders_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)
        logger.info(f"Successfully read {orders_df.count()} records")
        return orders_df
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        raise