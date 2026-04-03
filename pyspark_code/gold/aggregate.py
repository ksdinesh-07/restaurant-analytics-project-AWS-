"""
Gold Layer: Aggregate metrics for analytics
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoldAggregator:
    """Aggregate data for Gold layer metrics"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def calculate_daily_metrics(self, enriched_df):
        """Calculate daily restaurant metrics"""
        logger.info("Calculating daily metrics for Gold layer")
        
        daily_metrics = (
            enriched_df
            .where(col("status") == "DELIVERED")
            .groupBy("dt", "restaurant_id", "restaurant_name", "cuisine", "city")
            .agg(
                count("*").alias("orders_delivered"),
                sum("order_value").alias("gmv"),
                avg(
                    (unix_timestamp("delivered_ts") - unix_timestamp("order_ts")) / 60.0
                ).alias("avg_delivery_mins"),
                sum(when(col("late_delivery") == 1, 1).otherwise(0)).alias("late_count")
            )
            .withColumn("late_rate", col("late_count") / col("orders_delivered"))
            .withColumn("gmv_rounded", round(col("gmv"), 2))
        )
        
        logger.info(f"Calculated metrics for {daily_metrics.count()} restaurant-days")
        return daily_metrics
    
    def calculate_city_metrics(self, enriched_df):
        """Calculate city-level aggregated metrics"""
        logger.info("Calculating city-level metrics")
        
        city_metrics = (
            enriched_df
            .where(col("status") == "DELIVERED")
            .groupBy("dt", "city")
            .agg(
                countDistinct("restaurant_id").alias("active_restaurants"),
                count("*").alias("total_orders"),
                sum("order_value").alias("total_gmv"),
                avg(
                    (unix_timestamp("delivered_ts") - unix_timestamp("order_ts")) / 60.0
                ).alias("avg_delivery_time")
            )
        )
        
        return city_metrics
    
    def save_gold(self, df, output_path="s3a://restaurant-gold/daily_restaurant_metrics/"):
        """Save gold layer data to S3"""
        logger.info(f"Saving gold data to: {output_path}")
        
        df.write \
            .mode("overwrite") \
            .partitionBy("dt") \
            .parquet(output_path)
        
        logger.info("Gold data saved successfully")
        return output_path
    
    def show_sample(self, df, n=5):
        """Show sample of gold data"""
        logger.info(f"Showing {n} sample records from Gold layer")
        df.show(n, truncate=False)
        return df
