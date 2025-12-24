from spark_session import create_spark_session
from src.ingestion import read_orders_data
from src.aggregations import yearly_revenue, revenue_per_product
from src.transformations import transform_orders_data
from src.utils import write_parquet_file
from src.cleaning import clean_orders_data
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    Main ETL pipeline function that orchestrates data ingestion, cleaning,
    transformation, aggregation, and output writing.
    """
    spark = None
    try:
        # Create Spark session
        logger.info("Creating Spark session...")
        spark = create_spark_session()

        # Data ingestion
        logger.info("Reading raw orders data...")
        raw_df = read_orders_data(spark, "Data/raw/orders.csv")
        logger.info(f"Raw data count: {raw_df.count()}")

        # Data cleaning
        logger.info("Cleaning orders data...")
        clean_df = clean_orders_data(raw_df)
        logger.info(f"Cleaned data count: {clean_df.count()}")

        # Data transformation
        logger.info("Transforming orders data...")
        transform_df = transform_orders_data(clean_df)

        # Data aggregation
        logger.info("Aggregating revenue by product...")
        product_revenue_df = revenue_per_product(transform_df)
        logger.info("Aggregating revenue by year...")
        yearly_revenue_df = yearly_revenue(transform_df)

        # Write output
        logger.info("Writing product revenue to parquet...")
        write_parquet_file(product_revenue_df, "Data/processed/product_revenue.parquet")
        logger.info("Writing yearly revenue to parquet...")
        write_parquet_file(yearly_revenue_df, "Data/processed/yearly_revenue.parquet")
        
        logger.info("ETL pipeline completed successfully!")

    except Exception as e:
        logger.error(f"Error in ETL pipeline: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()

if __name__ == "__main__":
    main()