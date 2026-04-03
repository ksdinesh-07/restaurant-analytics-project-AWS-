"""
Main ETL Pipeline Orchestrator
"""

import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark_code.utils.spark_utils import create_spark_session, stop_spark_session, log_dataframe_info
from pyspark_code.bronze.load_data import BronzeLoader
from pyspark_code.silver.transform import SilverTransformer
from pyspark_code.gold.aggregate import GoldAggregator
from pyspark_code.config.config import S3_PATHS, PROCESSING_CONFIG
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLPipeline:
    """Main ETL Pipeline Orchestrator"""
    
    def __init__(self):
        self.spark = None
        self.bronze_loader = None
        self.silver_transformer = None
        self.gold_aggregator = None
    
    def initialize(self):
        """Initialize Spark and components"""
        logger.info("=" * 60)
        logger.info("Initializing ETL Pipeline")
        logger.info("=" * 60)
        
        self.spark = create_spark_session()
        self.bronze_loader = BronzeLoader(self.spark)
        self.silver_transformer = SilverTransformer(self.spark)
        self.gold_aggregator = GoldAggregator(self.spark)
        
        logger.info("✅ Pipeline initialized successfully")
    
    def run_bronze(self):
        """Execute Bronze layer"""
        logger.info("\n📦 Step 1: Loading Bronze Layer")
        orders, restaurants = self.bronze_loader.load_all()
        
        log_dataframe_info(orders, "Orders")
        log_dataframe_info(restaurants, "Restaurants")
        
        return orders, restaurants
    
    def run_silver(self, orders, restaurants):
        """Execute Silver layer"""
        logger.info("\n🔗 Step 2: Creating Silver Layer")
        
        enriched = self.silver_transformer.enrich_orders(orders, restaurants)
        output_path = self.silver_transformer.save_silver(
            enriched, 
            S3_PATHS['silver']['orders_enriched']
        )
        
        log_dataframe_info(enriched, "Enriched Orders")
        logger.info(f"Silver data saved to: {output_path}")
        
        return enriched
    
    def run_gold(self, enriched):
        """Execute Gold layer"""
        logger.info("\n🏆 Step 3: Creating Gold Layer")
        
        daily_metrics = self.gold_aggregator.calculate_daily_metrics(enriched)
        output_path = self.gold_aggregator.save_gold(
            daily_metrics,
            S3_PATHS['gold']['daily_metrics']
        )
        
        log_dataframe_info(daily_metrics, "Daily Metrics")
        logger.info(f"Gold data saved to: {output_path}")
        
        return daily_metrics
    
    def run_full_pipeline(self):
        """Run complete ETL pipeline"""
        try:
            # Execute all layers
            orders, restaurants = self.run_bronze()
            enriched = self.run_silver(orders, restaurants)
            gold = self.run_gold(enriched)
            
            # Show sample output
            logger.info("\n📊 Sample Gold Metrics:")
            self.gold_aggregator.show_sample(gold)
            
            logger.info("\n" + "=" * 60)
            logger.info("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("=" * 60)
            
            return gold
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}")
            raise
        finally:
            if self.spark:
                stop_spark_session(self.spark)

def main():
    """Main entry point"""
    pipeline = ETLPipeline()
    pipeline.initialize()
    pipeline.run_full_pipeline()

if __name__ == "__main__":
    main()
