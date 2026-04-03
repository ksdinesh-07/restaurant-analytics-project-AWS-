#!/usr/bin/env python3
"""
Restaurant Analytics ETL Pipeline
Bronze → Silver → Gold Layer Processing with Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    """Create Spark session with S3 configuration"""
    spark = SparkSession.builder \
        .appName("Restaurant ETL Pipeline") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com") \
        .getOrCreate()
    return spark

def load_bronze_data(spark):
    """Load raw data from S3 Bronze layer"""
    print("📥 Loading Bronze data...")
    
    orders = spark.read.csv(
        "s3a://restaurant-brz/orders/dt=2-4-2026/order.csv",
        header=True,
        inferSchema=True
    )
    
    restaurants = spark.read.csv(
        "s3a://restaurant-brz/restaurant/restaurant.csv",
        header=True,
        inferSchema=True
    )
    
    return orders, restaurants

def create_silver_layer(orders, restaurants):
    """Create enriched Silver layer with joins"""
    print("🔗 Creating Silver layer...")
    
    # Add date column
    orders_with_dt = orders.withColumn("dt", to_date(col("order_ts")))
    
    # Join with restaurants
    orders_enriched = orders_with_dt.alias("o").join(
        restaurants.alias("r"),
        col("o.restaurant_id") == col("r.restaurant_id"),
        "left"
    ).select(
        col("o.*"),
        col("r.name").alias("restaurant_name"),
        col("r.cuisine")
    )
    
    # Write Silver layer
    orders_enriched.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .parquet("s3a://restaurant-slvr/orders_enriched/")
    
    print("✅ Silver layer written to s3://restaurant-slvr/")
    return orders_enriched

def create_gold_layer(orders_enriched):
    """Create aggregated Gold layer metrics"""
    print("📊 Creating Gold layer...")
    
    daily_metrics = (
        orders_enriched
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
    )
    
    # Write Gold layer
    daily_metrics.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .parquet("s3a://restaurant-gold/daily_restaurant_metrics/")
    
    print("✅ Gold layer written to s3://restaurant-gold/")
    return daily_metrics

def main():
    """Main ETL pipeline execution"""
    print("=" * 60)
    print("🚀 STARTING RESTAURANT ANALYTICS ETL PIPELINE")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load Bronze data
        orders, restaurants = load_bronze_data(spark)
        print(f"📦 Orders: {orders.count()} records")
        print(f"📦 Restaurants: {restaurants.count()} records")
        
        # Create Silver layer
        silver_data = create_silver_layer(orders, restaurants)
        print(f"📈 Silver data: {silver_data.count()} records")
        
        # Create Gold layer
        gold_data = create_gold_layer(silver_data)
        print(f"🏆 Gold metrics: {gold_data.count()} records")
        
        # Show sample output
        print("\n📊 Sample Gold Metrics:")
        gold_data.show(5, truncate=False)
        
        print("\n" + "=" * 60)
        print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
