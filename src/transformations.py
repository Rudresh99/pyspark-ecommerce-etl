from pyspark.sql.functions import col, year
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def transform_orders_data(orders_df: DataFrame) -> DataFrame:
    """
    Transform the orders DataFrame by adding calculated columns.

    Parameters:
    orders_df (DataFrame): The Spark DataFrame containing cleaned orders data.

    Returns:
    DataFrame: A transformed Spark DataFrame with 'total_amount' and 'order_year' columns.
    
    Raises:
    Exception: If there's an error during transformation.
    """
    try:
        logger.info("Transforming orders data: calculating total_amount and order_year")
        transformed_df = orders_df \
            .withColumn("total_amount", col("price") * col("quantity")) \
            .withColumn("order_year", year(col("order_date")))
        
        logger.info("Transformation completed successfully")
        return transformed_df
    except Exception as e:
        logger.error(f"Error transforming orders data: {str(e)}")
        raise