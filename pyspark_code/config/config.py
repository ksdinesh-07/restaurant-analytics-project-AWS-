"""
Configuration settings for ETL pipeline
"""

# S3 Paths
S3_PATHS = {
    'bronze': {
        'orders': 's3a://restaurant-brz/orders/dt=2-4-2026/order.csv',
        'restaurants': 's3a://restaurant-brz/restaurant/restaurant.csv'
    },
    'silver': {
        'orders_enriched': 's3a://restaurant-slvr/orders_enriched/'
    },
    'gold': {
        'daily_metrics': 's3a://restaurant-gold/daily_restaurant_metrics/'
    }
}

# AWS Configuration
AWS_CONFIG = {
    'region': 'us-east-1',
    'endpoint': 's3.us-east-1.amazonaws.com'
}

# Spark Configuration
SPARK_CONFIG = {
    'app_name': 'Restaurant ETL Pipeline',
    'packages': [
        'org.apache.hadoop:hadoop-aws:3.3.4',
        'com.amazonaws:aws-java-sdk-bundle:1.12.641'
    ]
}

# Processing Configuration
PROCESSING_CONFIG = {
    'write_mode': 'overwrite',
    'partition_column': 'dt',
    'file_format': 'parquet'
}

# Business Rules
BUSINESS_RULES = {
    'status_filter': 'DELIVERED',
    'late_delivery_threshold_mins': 30
}
