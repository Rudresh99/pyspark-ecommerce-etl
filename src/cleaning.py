from pyspark.sql.functions import col, udf
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def safe_date_parse(date_str):
    """
    Safely parse a date string, trying multiple formats.
    Returns None if all formats fail.
    """
    if date_str is None:
        return None
    
    date_formats = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%d-%m-%Y',
        '%Y/%m/%d',
        '%d/%m/%Y',
        '%m-%d-%Y'
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str), fmt).date()
        except (ValueError, TypeError):
            continue
    
    return None

# Register UDF
safe_date_parse_udf = udf(safe_date_parse, DateType())

def clean_orders_data(orders_df: DataFrame) -> DataFrame:
    """
    Clean the orders DataFrame by removing duplicates and filtering out invalid records.

    Parameters:
    orders_df (DataFrame): The Spark DataFrame containing orders data.

    Returns:
    DataFrame: A cleaned Spark DataFrame with duplicates removed and invalid records filtered out.
    
    Raises:
    Exception: If there's an error during cleaning.
    """
    try:
        initial_count = orders_df.count()
        logger.info(f"Starting data cleaning. Initial record count: {initial_count}")
        
        # Remove duplicate records
        cleaned_df = orders_df.dropDuplicates(["order_id"])
        after_dedup_count = cleaned_df.count()
        logger.info(f"After removing duplicates: {after_dedup_count} records")

        # Drop rows where critical fields are missing
        cleaned_df = cleaned_df \
            .dropna(subset=["order_id","product_id","customer_id","order_date","price","quantity"])
        after_dropna_count = cleaned_df.count()
        logger.info(f"After dropping nulls: {after_dropna_count} records")

        # Filter out records with null or negative order amounts
        cleaned_df = cleaned_df.filter(
            (col("price").isNotNull()) & (col("price") >= 0) &
            (col("quantity").isNotNull()) & (col("quantity") > 0)
        )
        after_value_filter_count = cleaned_df.count()
        logger.info(f"After filtering invalid values: {after_value_filter_count} records")

        # Validate and filter out invalid dates using a safe UDF
        # The UDF tries multiple date formats and returns NULL for invalid dates
        cleaned_df = cleaned_df.withColumn(
            "order_date_parsed",
            safe_date_parse_udf(col("order_date"))
        )
        
        # Filter out rows with invalid dates and replace order_date with parsed version
        cleaned_df = cleaned_df.filter(col("order_date_parsed").isNotNull())
        cleaned_df = cleaned_df.drop("order_date").withColumnRenamed("order_date_parsed", "order_date")
        
        final_count = cleaned_df.count()
        logger.info(f"After filtering invalid dates: {final_count} records")
        logger.info(f"Removed {initial_count - final_count} invalid records total")

        return cleaned_df
    except Exception as e:
        logger.error(f"Error cleaning orders data: {str(e)}")
        raise