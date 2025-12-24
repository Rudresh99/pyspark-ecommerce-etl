from pyspark.sql.functions import sum
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def revenue_per_product(orders_df: DataFrame) -> DataFrame:
    """
    Aggregate the total revenue per product from the orders DataFrame.

    Parameters:
    orders_df (DataFrame): The Spark DataFrame containing transformed orders data.

    Returns:
    DataFrame: A Spark DataFrame with total revenue aggregated by product_id.
    
    Raises:
    Exception: If there's an error during aggregation.
    """
    try:
        logger.info("Aggregating revenue by product_id...")
        revenue_df = orders_df.groupBy("product_id") \
            .agg(sum("total_amount").alias("total_revenue"))
        logger.info(f"Aggregation complete. Found {revenue_df.count()} unique products")
        return revenue_df
    except Exception as e:
        logger.error(f"Error aggregating revenue per product: {str(e)}")
        raise

def yearly_revenue(orders_df: DataFrame) -> DataFrame:
    """
    Aggregate the total revenue per year from the orders DataFrame.

    Parameters:
    orders_df (DataFrame): The Spark DataFrame containing transformed orders data.

    Returns:
    DataFrame: A Spark DataFrame with total revenue aggregated by year.
    
    Raises:
    Exception: If there's an error during aggregation.
    """
    try:
        logger.info("Aggregating revenue by order_year...")
        yearly_revenue_df = orders_df.groupBy("order_year") \
            .agg(sum("total_amount").alias("yearly_revenue"))
        logger.info(f"Aggregation complete. Found {yearly_revenue_df.count()} years")
        return yearly_revenue_df
    except Exception as e:
        logger.error(f"Error aggregating yearly revenue: {str(e)}")
        raise
    
