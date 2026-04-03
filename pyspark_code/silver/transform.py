"""
Silver Layer: Enrich and transform data
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilverTransformer:
    """Transform and enrich data for Silver layer"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def add_date_column(self, df, date_column="order_ts", new_column="dt"):
        """Add date column from timestamp"""
        logger.info(f"Adding {new_column} column from {date_column}")
        return df.withColumn(new_column, to_date(col(date_column)))
    
    def enrich_orders(self, orders_df, restaurants_df, join_key="restaurant_id"):
        """Join orders with restaurant details"""
        logger.info(f"Enriching orders with restaurant data using {join_key}")
        
        # Add date column
        orders_with_dt = self.add_date_column(orders_df)
        
        # Join with restaurants
        enriched = orders_with_dt.alias("o").join(
            restaurants_df.alias("r"),
            col(f"o.{join_key}") == col(f"r.{join_key}"),
            "left"
        ).select(
            col("o.*"),
            col("r.name").alias("restaurant_name"),
            col("r.cuisine")
        )
        
        logger.info(f"Enriched data has {enriched.count()} records")
        return enriched
    
    def save_silver(self, df, output_path="s3a://restaurant-slvr/orders_enriched/"):
        """Save silver layer data to S3"""
        logger.info(f"Saving silver data to: {output_path}")
        
        df.write \
            .mode("overwrite") \
            .partitionBy("dt") \
            .parquet(output_path)
        
        logger.info("Silver data saved successfully")
        return output_path
