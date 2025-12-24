from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def write_parquet_file(orders_df: DataFrame, file_path: str) -> None:
    """
    Writes a Spark DataFrame to a Parquet file.

    Parameters:
    orders_df (DataFrame): The Spark DataFrame to be written to the Parquet file.
    file_path (str): The path where the Parquet file will be saved.
    
    Raises:
    Exception: If there's an error writing the Parquet file.
    """
    try:
        logger.info(f"Writing parquet file to: {file_path}")
        orders_df.write \
            .mode("overwrite") \
            .parquet(file_path)
        logger.info(f"Successfully wrote parquet file to: {file_path}")
    except Exception as e:
        logger.error(f"Error writing parquet file to {file_path}: {str(e)}")
        raise