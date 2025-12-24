from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def create_spark_session(app_name: str = "EcommerceETL", master: str = "local[*]") -> SparkSession:
    """
    Create and configure a Spark session for ETL operations.

    Parameters:
    app_name (str): Name of the Spark application. Defaults to "EcommerceETL".
    master (str): Spark master URL. Defaults to "local[*]".

    Returns:
    SparkSession: A configured Spark session.
    """
    try:
        logger.info(f"Creating Spark session: {app_name} on {master}")
        spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        raise
