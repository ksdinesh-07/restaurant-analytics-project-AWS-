"""
Bronze Layer: Load raw data from S3
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeLoader:
    """Load raw data from Bronze layer S3 buckets"""
    
    def __init__(self, spark):
        self.spark = spark
        
    def load_orders(self, path="s3a://restaurant-brz/orders/dt=2-4-2026/order.csv"):
        """Load orders data from CSV"""
        logger.info(f"Loading orders from: {path}")
        df = self.spark.read.csv(path, header=True, inferSchema=True)
        logger.info(f"Loaded {df.count()} orders")
        return df
    
    def load_restaurants(self, path="s3a://restaurant-brz/restaurant/restaurant.csv"):
        """Load restaurants data from CSV"""
        logger.info(f"Loading restaurants from: {path}")
        df = self.spark.read.csv(path, header=True, inferSchema=True)
        logger.info(f"Loaded {df.count()} restaurants")
        return df
    
    def load_all(self):
        """Load all bronze data"""
        orders = self.load_orders()
        restaurants = self.load_restaurants()
        return orders, restaurants
